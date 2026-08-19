package local

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/internal/structuredlog"
)

func TestClientCreateOpenWriteReadRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	client, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:        service.HexVolumeID(101),
		Name:      "vol-a",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	openResp, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-1",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume failed: %v", err)
	}

	writeReq := &service.WriteRequest{
		VolumeID:     service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle: openResp.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  4096,
		Data:         make([]byte, 4096),
		Context: service.SBSRequestContext{
			RequestID:      "req-write-1",
			GatewayID:      "gw-a",
			AttachmentID:   "att-00000065-0001",
			Generation:     1,
			IdempotencyKey: "idem-write-1",
		},
	}
	writeReq.Data[0] = 0xAA
	writeResp, err := client.Write(context.Background(), writeReq)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	client, err = Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("reopen Open failed: %v", err)
	}
	defer client.Close()
	if _, err := client.CreateVolume(context.Background(), spec); err != nil {
		t.Fatalf("CreateVolume after restart failed: %v", err)
	}

	openResp2, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-2",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume after restart failed: %v", err)
	}

	readResp, err := client.Read(context.Background(), &service.ReadRequest{
		VolumeID:     service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle: openResp2.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  4096,
		Context: service.SBSRequestContext{
			RequestID:    "req-read-1",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(readResp.Data) != 4096 || readResp.Data[0] != 0xAA {
		t.Fatalf("unexpected read payload")
	}

	writeResp2, err := client.Write(context.Background(), writeReq)
	if err != nil {
		t.Fatalf("idempotent Write after restart failed: %v", err)
	}
	if writeResp2.CommitID != writeResp.CommitID || writeResp2.VolumeRevision != writeResp.VolumeRevision {
		t.Fatalf("expected same write result after restart: before=%+v after=%+v", writeResp, writeResp2)
	}
}

func TestClientWritePhysicalChunkEmitsStructuredLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	client, err := Open(Config{Path: dir, TraceDataOperations: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer client.Close()

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:              service.HexVolumeID(102),
		Name:            "vol-physical-log",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	openResp, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-physical-log",
			GatewayID:    "gw-a",
			HostID:       "host-a",
			SessionID:    "session-rep-a",
			AttachmentID: "att-00000066-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume failed: %v", err)
	}

	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()

	resp, err := client.WritePhysicalChunk(context.Background(), &service.WritePhysicalChunkRequest{
		VolumeID:         service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle:     openResp.VolumeHandle,
		PhysicalChunkID:  9,
		ChunkOffsetBytes: 4,
		LengthBytes:      4,
		Data:             []byte("DATA"),
		Context: service.SBSRequestContext{
			RequestID:      "req-physical-write-1",
			GatewayID:      "gw-a",
			HostID:         "host-a",
			SessionID:      "session-rep-a",
			AttachmentID:   "att-00000066-0001",
			Generation:     1,
			IdempotencyKey: "idem-physical-write-1",
		},
	})
	if err != nil {
		t.Fatalf("WritePhysicalChunk failed: %v", err)
	}
	if resp.VolumeRevision == 0 {
		t.Fatalf("expected non-zero revision")
	}

	readResp, err := client.ReadPhysicalChunk(context.Background(), &service.ReadPhysicalChunkRequest{
		VolumeID:         service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle:     openResp.VolumeHandle,
		PhysicalChunkID:  9,
		ChunkOffsetBytes: 0,
		LengthBytes:      16,
		Context: service.SBSRequestContext{
			RequestID:    "req-physical-read-1",
			GatewayID:    "gw-a",
			HostID:       "host-a",
			SessionID:    "session-rep-a",
			AttachmentID: "att-00000066-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("ReadPhysicalChunk failed: %v", err)
	}
	wantPayload := []byte{0, 0, 0, 0, 'D', 'A', 'T', 'A', 0, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(readResp.Data, wantPayload) {
		t.Fatalf("unexpected physical chunk payload: got=%q want=%q", readResp.Data, wantPayload)
	}

	logs := buf.String()
	for _, want := range []string{
		`"component":"sbs.data"`,
		`"event":"local_physical_chunk_write_completed"`,
		`"request_id":"req-physical-write-1"`,
		`"gateway_id":"gw-a"`,
		`"host_id":"host-a"`,
		`"session_id":"session-rep-a"`,
		`"physical_chunk_id":9`,
		`"physical_chunk_offset_bytes":4`,
		`"data_write_chunk_read_duration_ms":`,
		`"data_write_chunk_payload_duration_ms":`,
		`"data_write_chunks_read":1`,
		`"data_write_chunks_written":1`,
		`"revision_bump_duration_ms":`,
		`"revision_bump_lock_wait_duration_ms":`,
		`"revision_bump_state_get_duration_ms":`,
		`"revision_bump_state_put_duration_ms":`,
		`"revision_bump_critical_section_duration_ms":`,
		`"idempotency_store_duration_ms":`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs missing %q: %s", want, logs)
		}
	}
}

func TestClientWritePhysicalChunkSuppressesSuccessTraceByDefault(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	client, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer client.Close()

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:              service.HexVolumeID(105),
		Name:            "vol-physical-log-default",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	openResp, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-physical-log-default",
			GatewayID:    "gw-a",
			HostID:       "host-a",
			SessionID:    "session-rep-a",
			AttachmentID: "att-00000069-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume failed: %v", err)
	}

	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()

	_, err = client.WritePhysicalChunk(context.Background(), &service.WritePhysicalChunkRequest{
		VolumeID:         service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle:     openResp.VolumeHandle,
		PhysicalChunkID:  11,
		ChunkOffsetBytes: 0,
		LengthBytes:      4,
		Data:             []byte("DATA"),
		Context: service.SBSRequestContext{
			RequestID:      "req-physical-write-default",
			GatewayID:      "gw-a",
			HostID:         "host-a",
			SessionID:      "session-rep-a",
			AttachmentID:   "att-00000069-0001",
			Generation:     1,
			IdempotencyKey: "idem-physical-write-default",
		},
	})
	if err != nil {
		t.Fatalf("WritePhysicalChunk failed: %v", err)
	}
	if got := strings.TrimSpace(buf.String()); got != "" {
		t.Fatalf("default WritePhysicalChunk emitted success trace: %s", got)
	}
}

func TestOpenConfiguresLabIdempotencySyncMode(t *testing.T) {
	synced, err := Open(Config{Path: filepath.Join(t.TempDir(), "synced")})
	if err != nil {
		t.Fatalf("Open synced failed: %v", err)
	}
	if synced.meta.idempotencyWriteOptions != pebble.Sync {
		t.Fatalf("default idempotency write options=%p want pebble.Sync", synced.meta.idempotencyWriteOptions)
	}
	if err := synced.Close(); err != nil {
		t.Fatalf("Close synced failed: %v", err)
	}

	noSync, err := Open(Config{Path: filepath.Join(t.TempDir(), "nosync"), DisableIdempotencySync: true})
	if err != nil {
		t.Fatalf("Open nosync failed: %v", err)
	}
	defer noSync.Close()
	if noSync.meta.idempotencyWriteOptions != pebble.NoSync {
		t.Fatalf("disabled idempotency write options=%p want pebble.NoSync", noSync.meta.idempotencyWriteOptions)
	}
}

func TestOpenConfiguresLabPhysicalWriteFastPaths(t *testing.T) {
	client, err := Open(Config{
		Path:                            filepath.Join(t.TempDir(), "fastpath"),
		CacheOpenVolumeSpec:             true,
		DisablePhysicalWriteIdempotency: true,
		TraceDataOperations:             true,
	})
	if err != nil {
		t.Fatalf("Open fastpath failed: %v", err)
	}
	defer client.Close()
	if !client.cacheOpenVolumeSpec {
		t.Fatal("cacheOpenVolumeSpec = false, want true")
	}
	if !client.disablePhysicalWriteIdempotency {
		t.Fatal("disablePhysicalWriteIdempotency = false, want true")
	}
	if !client.traceDataOperations {
		t.Fatal("traceDataOperations = false, want true")
	}
}

func TestClientLabPhysicalWriteFastPathSkipsIdempotencyRecords(t *testing.T) {
	client, err := Open(Config{
		Path:                            filepath.Join(t.TempDir(), "pebble"),
		CacheOpenVolumeSpec:             true,
		DisablePhysicalWriteIdempotency: true,
		TraceDataOperations:             true,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer client.Close()

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:              service.HexVolumeID(104),
		Name:            "vol-physical-fastpath",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	volumeID := service.CanonicalVolumeID(uint64(spec.ID))
	openResp, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   volumeID,
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-physical-fastpath",
			GatewayID:    "gw-a",
			HostID:       "host-a",
			SessionID:    "session-rep-a",
			AttachmentID: "att-00000068-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume failed: %v", err)
	}

	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()
	_, err = client.WritePhysicalChunk(context.Background(), &service.WritePhysicalChunkRequest{
		VolumeID:         volumeID,
		VolumeHandle:     openResp.VolumeHandle,
		PhysicalChunkID:  31,
		ChunkOffsetBytes: 0,
		LengthBytes:      16,
		Data:             []byte("0123456789abcdef"),
		Context: service.SBSRequestContext{
			RequestID:      "req-physical-fastpath-1",
			GatewayID:      "gw-a",
			HostID:         "host-a",
			SessionID:      "session-rep-a",
			AttachmentID:   "att-00000068-0001",
			Generation:     1,
			IdempotencyKey: "idem-physical-fastpath-1",
		},
	})
	if err != nil {
		t.Fatalf("WritePhysicalChunk failed: %v", err)
	}
	if !strings.Contains(buf.String(), `"idempotency_fast_path":true`) {
		t.Fatalf("logs missing idempotency fast path marker: %s", buf.String())
	}
	snapshot, err := client.ObservabilitySnapshot()
	if err != nil {
		t.Fatalf("ObservabilitySnapshot failed: %v", err)
	}
	if snapshot.IdempotencyRecords != 0 {
		t.Fatalf("idempotency records = %d, want 0", snapshot.IdempotencyRecords)
	}
}

func TestClientWritePhysicalChunkUsesObservedRevisionWithoutDurableBump(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	client, err := Open(Config{Path: dir, TraceDataOperations: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer client.Close()

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:              service.HexVolumeID(103),
		Name:            "vol-physical-observed-revision",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	volumeID := service.CanonicalVolumeID(uint64(spec.ID))
	openResp, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   volumeID,
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-physical-observed",
			GatewayID:    "gw-a",
			HostID:       "host-a",
			SessionID:    "session-rep-a",
			AttachmentID: "att-00000067-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume failed: %v", err)
	}
	initialState, err := client.meta.getVolumeState(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("get initial state failed: %v", err)
	}
	counter := client.meta.observedRevisionCounter(volumeID)
	if !counter.initialized.Load() {
		t.Fatal("observed revision counter was not initialized by OpenVolume")
	}
	if got := counter.value.Load(); got != initialState.VolumeRevision {
		t.Fatalf("observed revision counter=%d want initial state revision %d", got, initialState.VolumeRevision)
	}

	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()

	first, err := client.WritePhysicalChunk(context.Background(), &service.WritePhysicalChunkRequest{
		VolumeID:         volumeID,
		VolumeHandle:     openResp.VolumeHandle,
		PhysicalChunkID:  21,
		ChunkOffsetBytes: 0,
		LengthBytes:      4,
		Data:             []byte("ABCD"),
		Context: service.SBSRequestContext{
			RequestID:      "req-physical-observed-1",
			GatewayID:      "gw-a",
			HostID:         "host-a",
			SessionID:      "session-rep-a",
			AttachmentID:   "att-00000067-0001",
			Generation:     1,
			IdempotencyKey: "idem-physical-observed-1",
		},
	})
	if err != nil {
		t.Fatalf("first WritePhysicalChunk failed: %v", err)
	}
	second, err := client.WritePhysicalChunk(context.Background(), &service.WritePhysicalChunkRequest{
		VolumeID:         volumeID,
		VolumeHandle:     openResp.VolumeHandle,
		PhysicalChunkID:  22,
		ChunkOffsetBytes: 0,
		LengthBytes:      4,
		Data:             []byte("EFGH"),
		Context: service.SBSRequestContext{
			RequestID:      "req-physical-observed-2",
			GatewayID:      "gw-a",
			HostID:         "host-a",
			SessionID:      "session-rep-a",
			AttachmentID:   "att-00000067-0001",
			Generation:     1,
			IdempotencyKey: "idem-physical-observed-2",
		},
	})
	if err != nil {
		t.Fatalf("second WritePhysicalChunk failed: %v", err)
	}
	if first.VolumeRevision <= initialState.VolumeRevision {
		t.Fatalf("first observed revision=%d want > initial %d", first.VolumeRevision, initialState.VolumeRevision)
	}
	if second.VolumeRevision <= first.VolumeRevision {
		t.Fatalf("second observed revision=%d want > first %d", second.VolumeRevision, first.VolumeRevision)
	}
	afterPhysicalState, err := client.meta.getVolumeState(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("get state after physical writes failed: %v", err)
	}
	if afterPhysicalState.VolumeRevision != initialState.VolumeRevision {
		t.Fatalf("physical writes persisted state revision=%d want unchanged %d", afterPhysicalState.VolumeRevision, initialState.VolumeRevision)
	}

	logical, err := client.Write(context.Background(), &service.WriteRequest{
		VolumeID:     volumeID,
		VolumeHandle: openResp.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  4,
		Data:         []byte("IJKL"),
		Context: service.SBSRequestContext{
			RequestID:      "req-logical-after-physical-observed",
			GatewayID:      "gw-a",
			HostID:         "host-a",
			SessionID:      "session-rep-a",
			AttachmentID:   "att-00000067-0001",
			Generation:     1,
			IdempotencyKey: "idem-logical-after-physical-observed",
		},
	})
	if err != nil {
		t.Fatalf("logical Write failed: %v", err)
	}
	if logical.VolumeRevision <= second.VolumeRevision {
		t.Fatalf("logical revision=%d want > observed physical revision %d", logical.VolumeRevision, second.VolumeRevision)
	}
	afterLogicalState, err := client.meta.getVolumeState(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("get state after logical write failed: %v", err)
	}
	if afterLogicalState.VolumeRevision != logical.VolumeRevision {
		t.Fatalf("durable logical revision=%d want %d", afterLogicalState.VolumeRevision, logical.VolumeRevision)
	}

	logs := buf.String()
	for _, want := range []string{
		`"event":"local_physical_chunk_write_completed"`,
		`"revision_bump_mode":"observed_in_memory"`,
		`"event":"local_write_completed"`,
		`"revision_bump_mode":"persisted_state"`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs missing %q: %s", want, logs)
		}
	}
}

func TestObservedPhysicalRevisionInitializationDoesNotLowerExistingCounter(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	client, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer client.Close()

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:              service.HexVolumeID(104),
		Name:            "vol-physical-observed-init-race",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	volumeID := service.CanonicalVolumeID(uint64(spec.ID))
	counter := client.meta.observedRevisionCounter(volumeID)
	counter.value.Store(10)

	client.meta.initializeObservedVolumeRevisionAtLeast(volumeID, 1)
	next := counter.value.Add(1)
	if next != 11 {
		t.Fatalf("observed revision counter moved backward, next=%d want 11", next)
	}
}

func TestClientReadUnwrittenVolumeReturnsZeroFilled(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	client, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer client.Close()

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:        service.HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 1 << 20,
		BlockSize: 4096,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	openResp, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-sparse-1",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume failed: %v", err)
	}

	readResp, err := client.Read(context.Background(), &service.ReadRequest{
		VolumeID:     service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle: openResp.VolumeHandle,
		OffsetBytes:  32 * 512,
		LengthBytes:  4096,
		Context: service.SBSRequestContext{
			RequestID:    "req-read-sparse-sector-32",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("Read unwritten sparse range failed: %v", err)
	}
	if len(readResp.Data) != 4096 {
		t.Fatalf("read len=%d want=4096", len(readResp.Data))
	}
	for i, b := range readResp.Data {
		if b != 0 {
			t.Fatalf("read byte[%d]=%#x want zero", i, b)
		}
	}
}

func TestTranslateServiceErrorMapsRawPebbleNotFoundToSBSNotFound(t *testing.T) {
	err := translateServiceError(pebble.ErrNotFound)
	var sbsErr *service.SBSError
	if !errors.As(err, &sbsErr) {
		t.Fatalf("error type=%T want *service.SBSError", err)
	}
	if sbsErr.Code != service.SBSErrorCodeNotFound {
		t.Fatalf("sbs error code=%s want %s", sbsErr.Code, service.SBSErrorCodeNotFound)
	}
}

func TestClientCreateOpenWriteReadRestartWithPrefix(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	client, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:        service.HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	openResp, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-1",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume failed: %v", err)
	}

	writeReq := &service.WriteRequest{
		VolumeID:     service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle: openResp.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  4096,
		Data:         make([]byte, 4096),
		Context: service.SBSRequestContext{
			RequestID:      "req-write-1",
			GatewayID:      "gw-a",
			AttachmentID:   "att-00000065-0001",
			Generation:     1,
			IdempotencyKey: "idem-write-1",
		},
	}
	writeReq.Data[0] = 0xAA
	writeReq.Data[4095] = 0xFE
	if _, err := client.Write(context.Background(), writeReq); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	client, err = Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("reopen Open failed: %v", err)
	}
	defer client.Close()

	openResp2, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-2",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume after restart failed: %v", err)
	}

	readResp, err := client.Read(context.Background(), &service.ReadRequest{
		VolumeID:     service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle: openResp2.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  4096,
		Context: service.SBSRequestContext{
			RequestID:    "req-read-1",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(readResp.Data) != 4096 || readResp.Data[0] != 0xAA || readResp.Data[4095] != 0xFE {
		t.Fatalf("unexpected read payload len=%d first=0x%02x last=0x%02x", len(readResp.Data), readResp.Data[0], readResp.Data[4095])
	}
}

func TestClientSweepChunkGarbageHonorsProtectedRefs(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	client, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer client.Close()

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:        service.HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	if err := client.objects.Put(context.Background(), store.BuildChunkKey(spec.Prefix, 1), make([]byte, service.DefaultAllocationChunkSize)); err != nil {
		t.Fatalf("Put protected chunk failed: %v", err)
	}
	if err := client.meta.PutChunkGarbage(context.Background(), service.AllocationChunkGarbageRecord{
		VolumeID: spec.ID,
		ChunkID:  1,
	}); err != nil {
		t.Fatalf("PutChunkGarbage failed: %v", err)
	}

	result, err := client.SweepChunkGarbage(context.Background(), service.CanonicalVolumeID(uint64(spec.ID)), 16, []service.PhysicalChunkRef{{ChunkID: 1}})
	if err != nil {
		t.Fatalf("SweepChunkGarbage protected failed: %v", err)
	}
	if result.DeletedCount != 0 || result.RetainedCount != 1 {
		t.Fatalf("protected sweep result=%+v", result)
	}
	if _, found, err := client.objects.Get(context.Background(), store.BuildChunkKey(spec.Prefix, 1)); err != nil || !found {
		t.Fatalf("expected protected chunk to remain found=%v err=%v", found, err)
	}

	result, err = client.SweepChunkGarbage(context.Background(), service.CanonicalVolumeID(uint64(spec.ID)), 16, nil)
	if err != nil {
		t.Fatalf("SweepChunkGarbage unprotected failed: %v", err)
	}
	if result.DeletedCount != 1 || result.RetainedCount != 0 {
		t.Fatalf("unprotected sweep result=%+v", result)
	}
	if _, found, err := client.objects.Get(context.Background(), store.BuildChunkKey(spec.Prefix, 1)); err != nil {
		t.Fatalf("Get deleted chunk err=%v", err)
	} else if found {
		t.Fatalf("expected unprotected chunk to be deleted")
	}
}

func TestClientPurgeVolumeDeletesPayloadAndRejectsOpenSession(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	client, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer client.Close()

	volumeID := service.CanonicalVolumeID(101)
	spec := service.VolumeSpec{ID: service.HexVolumeID(101), Prefix: "payload-prefix", SizeBytes: 4096, BlockSize: 4096}
	if err := client.meta.putJSON(specKey(volumeID), spec, pebble.Sync); err != nil {
		t.Fatalf("Put volume spec failed: %v", err)
	}
	if err := client.objects.Put(context.Background(), store.BuildChunkKey(spec.Prefix, 1), []byte("payload")); err != nil {
		t.Fatalf("Put payload failed: %v", err)
	}
	client.mu.Lock()
	client.open["handle"] = openSession{spec: service.VolumeSpec{ID: service.HexVolumeID(101)}}
	client.mu.Unlock()
	if _, err := client.PurgeVolume(context.Background(), volumeID); err == nil {
		t.Fatal("PurgeVolume succeeded with an open session")
	}
	client.mu.Lock()
	delete(client.open, "handle")
	client.mu.Unlock()

	result, err := client.PurgeVolume(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("PurgeVolume failed: %v", err)
	}
	if result.KeyCount < 2 || result.ReclaimedBytes < uint64(len("payload")) {
		t.Fatalf("PurgeVolume result=%+v", result)
	}
	if _, found, err := client.objects.Get(context.Background(), store.BuildChunkKey(spec.Prefix, 1)); err != nil || found {
		t.Fatalf("payload remained found=%v err=%v", found, err)
	}
}

func TestClientRejectsIdempotencyConflict(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	client, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer client.Close()

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:        service.HexVolumeID(101),
		Name:      "vol-a",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	openResp, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-1",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume failed: %v", err)
	}

	reqA := &service.WriteRequest{
		VolumeID:     service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle: openResp.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  4096,
		Data:         make([]byte, 4096),
		Context: service.SBSRequestContext{
			RequestID:      "req-write-a",
			GatewayID:      "gw-a",
			AttachmentID:   "att-00000065-0001",
			Generation:     1,
			IdempotencyKey: "idem-write-1",
		},
	}
	reqB := &service.WriteRequest{
		VolumeID:     service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle: openResp.VolumeHandle,
		OffsetBytes:  4096,
		LengthBytes:  4096,
		Data:         make([]byte, 4096),
		Context:      reqA.Context,
	}

	if _, err := client.Write(context.Background(), reqA); err != nil {
		t.Fatalf("first Write failed: %v", err)
	}
	if _, err := client.Write(context.Background(), reqB); err == nil {
		t.Fatalf("expected idempotency conflict")
	}
}

func TestClientRejectsStaleGenerationAfterReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	client, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer client.Close()

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:        service.HexVolumeID(101),
		Name:      "vol-a",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	openResp, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-1",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume failed: %v", err)
	}

	if _, err := client.CloseVolume(context.Background(), &service.CloseVolumeRequest{
		VolumeID:     service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle: openResp.VolumeHandle,
		Context: service.SBSRequestContext{
			RequestID:    "req-close-1",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   1,
		},
	}); err != nil {
		t.Fatalf("CloseVolume failed: %v", err)
	}

	if _, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-2",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   2,
		},
	}); err != nil {
		t.Fatalf("OpenVolume with bumped generation failed: %v", err)
	}

	_, err = client.Write(context.Background(), &service.WriteRequest{
		VolumeID:     service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle: openResp.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  4096,
		Data:         make([]byte, 4096),
		Context: service.SBSRequestContext{
			RequestID:      "req-write-stale",
			GatewayID:      "gw-a",
			AttachmentID:   "att-00000065-0001",
			Generation:     1,
			IdempotencyKey: "idem-stale",
		},
	})
	if err == nil {
		t.Fatalf("expected stale generation error")
	}
	var sbsErr *service.SBSError
	if !errors.As(err, &sbsErr) || sbsErr.Code != service.SBSErrorCodeStaleGeneration {
		t.Fatalf("expected stale_generation error, got %v", err)
	}
}
