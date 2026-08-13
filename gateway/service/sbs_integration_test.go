package service

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	namrbdversion "github.com/nosway/namrbd/version"
)

type strictOpenSBSClient struct {
	mu      sync.Mutex
	volumes map[string]strictOpenSBSVolume
}

type strictOpenSBSVolume struct {
	spec       VolumeSpec
	data       []byte
	revision   uint64
	handle     string
	attachment string
	generation uint64
}

type unavailableOnceSBSClient struct {
	next serviceSBSClient

	mu        sync.Mutex
	openCalls int
}

type fakeInitialZeroMapResolver struct {
	pages []clustermeta.ResolvedAllocationPage
	err   error
}

func (r fakeInitialZeroMapResolver) ResolveAllocationPages(context.Context, string, uint64, uint64, uint32, uint32) ([]clustermeta.ResolvedAllocationPage, error) {
	return r.pages, r.err
}

type serviceSBSClient interface {
	OpenVolume(ctx context.Context, req *OpenVolumeRequest) (*OpenVolumeResponse, error)
	CloseVolume(ctx context.Context, req *CloseVolumeRequest) (*CloseVolumeResponse, error)
	GetVolumeProfile(ctx context.Context, req *GetVolumeProfileRequest) (*GetVolumeProfileResponse, error)
	GetVolumeStatus(ctx context.Context, req *GetVolumeStatusRequest) (*GetVolumeStatusResponse, error)
	Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error)
	Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error)
	Flush(ctx context.Context, req *FlushRequest) (*FlushResponse, error)
	Discard(ctx context.Context, req *DiscardRequest) (*DiscardResponse, error)
	Zero(ctx context.Context, req *ZeroRequest) (*ZeroResponse, error)
}

func newUnavailableOnceSBSClient(next serviceSBSClient) *unavailableOnceSBSClient {
	return &unavailableOnceSBSClient{next: next}
}

func (c *unavailableOnceSBSClient) OpenVolume(ctx context.Context, req *OpenVolumeRequest) (*OpenVolumeResponse, error) {
	c.mu.Lock()
	c.openCalls++
	call := c.openCalls
	c.mu.Unlock()
	if call == 1 {
		return nil, &SBSError{Code: SBSErrorCodeUnavailable, Message: "temporary unavailable", Retryable: true}
	}
	return c.next.OpenVolume(ctx, req)
}

func (c *unavailableOnceSBSClient) CloseVolume(ctx context.Context, req *CloseVolumeRequest) (*CloseVolumeResponse, error) {
	return c.next.CloseVolume(ctx, req)
}

func (c *unavailableOnceSBSClient) GetVolumeProfile(ctx context.Context, req *GetVolumeProfileRequest) (*GetVolumeProfileResponse, error) {
	return c.next.GetVolumeProfile(ctx, req)
}

func (c *unavailableOnceSBSClient) GetVolumeStatus(ctx context.Context, req *GetVolumeStatusRequest) (*GetVolumeStatusResponse, error) {
	return c.next.GetVolumeStatus(ctx, req)
}

func (c *unavailableOnceSBSClient) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	return c.next.Read(ctx, req)
}

func (c *unavailableOnceSBSClient) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	return c.next.Write(ctx, req)
}

func (c *unavailableOnceSBSClient) Flush(ctx context.Context, req *FlushRequest) (*FlushResponse, error) {
	return c.next.Flush(ctx, req)
}

func (c *unavailableOnceSBSClient) Discard(ctx context.Context, req *DiscardRequest) (*DiscardResponse, error) {
	return c.next.Discard(ctx, req)
}

func (c *unavailableOnceSBSClient) Zero(ctx context.Context, req *ZeroRequest) (*ZeroResponse, error) {
	return c.next.Zero(ctx, req)
}

type writeDataAliasObservingClient struct {
	serviceSBSClient
	expected []byte
	aliased  bool
}

func (c *writeDataAliasObservingClient) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	c.aliased = len(c.expected) > 0 && len(req.Data) > 0 && &req.Data[0] == &c.expected[0]
	return c.serviceSBSClient.Write(ctx, req)
}

type failOnceIOClient struct {
	next serviceSBSClient

	mu            sync.Mutex
	failReadOnce  bool
	failWriteOnce bool
	failFlushOnce bool
}

func newFailOnceIOClient(next serviceSBSClient, failReadOnce, failWriteOnce, failFlushOnce bool) *failOnceIOClient {
	return &failOnceIOClient{
		next:          next,
		failReadOnce:  failReadOnce,
		failWriteOnce: failWriteOnce,
		failFlushOnce: failFlushOnce,
	}
}

func (c *failOnceIOClient) OpenVolume(ctx context.Context, req *OpenVolumeRequest) (*OpenVolumeResponse, error) {
	return c.next.OpenVolume(ctx, req)
}

func (c *failOnceIOClient) CloseVolume(ctx context.Context, req *CloseVolumeRequest) (*CloseVolumeResponse, error) {
	return c.next.CloseVolume(ctx, req)
}

func (c *failOnceIOClient) GetVolumeProfile(ctx context.Context, req *GetVolumeProfileRequest) (*GetVolumeProfileResponse, error) {
	return c.next.GetVolumeProfile(ctx, req)
}

func (c *failOnceIOClient) GetVolumeStatus(ctx context.Context, req *GetVolumeStatusRequest) (*GetVolumeStatusResponse, error) {
	return c.next.GetVolumeStatus(ctx, req)
}

func (c *failOnceIOClient) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	c.mu.Lock()
	fail := c.failReadOnce
	if c.failReadOnce {
		c.failReadOnce = false
	}
	c.mu.Unlock()
	if fail {
		return nil, &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "attachment mismatch"}
	}
	return c.next.Read(ctx, req)
}

func (c *failOnceIOClient) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	c.mu.Lock()
	fail := c.failWriteOnce
	if c.failWriteOnce {
		c.failWriteOnce = false
	}
	c.mu.Unlock()
	if fail {
		return nil, &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "attachment mismatch"}
	}
	return c.next.Write(ctx, req)
}

func (c *failOnceIOClient) Flush(ctx context.Context, req *FlushRequest) (*FlushResponse, error) {
	c.mu.Lock()
	fail := c.failFlushOnce
	if c.failFlushOnce {
		c.failFlushOnce = false
	}
	c.mu.Unlock()
	if fail {
		return nil, &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "attachment mismatch"}
	}
	return c.next.Flush(ctx, req)
}

func (c *failOnceIOClient) Discard(ctx context.Context, req *DiscardRequest) (*DiscardResponse, error) {
	c.mu.Lock()
	fail := c.failWriteOnce
	if c.failWriteOnce {
		c.failWriteOnce = false
	}
	c.mu.Unlock()
	if fail {
		return nil, &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "attachment mismatch"}
	}
	return c.next.Discard(ctx, req)
}

func (c *failOnceIOClient) Zero(ctx context.Context, req *ZeroRequest) (*ZeroResponse, error) {
	c.mu.Lock()
	fail := c.failWriteOnce
	if c.failWriteOnce {
		c.failWriteOnce = false
	}
	c.mu.Unlock()
	if fail {
		return nil, &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "attachment mismatch"}
	}
	return c.next.Zero(ctx, req)
}

func newStrictOpenSBSClient(volumes []VolumeSpec) *strictOpenSBSClient {
	client := &strictOpenSBSClient{volumes: make(map[string]strictOpenSBSVolume, len(volumes))}
	for _, spec := range volumes {
		spec = NormalizeVolumeSpec(spec)
		id := CanonicalVolumeID(uint64(spec.ID))
		client.volumes[id] = strictOpenSBSVolume{
			spec:     spec,
			data:     make([]byte, spec.SizeBytes),
			revision: 1,
		}
	}
	return client
}

func (c *strictOpenSBSClient) OpenVolume(_ context.Context, req *OpenVolumeRequest) (*OpenVolumeResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if vol.handle != "" && (vol.attachment != req.Context.AttachmentID || vol.generation != req.Context.Generation) {
		return nil, &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "volume already opened by different writer context"}
	}
	vol.attachment = req.Context.AttachmentID
	vol.generation = req.Context.Generation
	vol.handle = "strict-" + req.VolumeID + "-" + req.Context.AttachmentID
	c.volumes[req.VolumeID] = vol
	return &OpenVolumeResponse{
		Status:         "ok",
		VolumeHandle:   vol.handle,
		VolumeID:       req.VolumeID,
		VolumeRevision: vol.revision,
		Profile:        profileFromSpec(vol.spec),
		ServerVersion:  namrbdversion.Current,
	}, nil
}

func (c *strictOpenSBSClient) CloseVolume(_ context.Context, req *CloseVolumeRequest) (*CloseVolumeResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if vol.attachment != req.Context.AttachmentID || vol.generation != req.Context.Generation {
		return nil, &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "attachment mismatch"}
	}
	vol.handle = ""
	c.volumes[req.VolumeID] = vol
	return &CloseVolumeResponse{Status: "ok"}, nil
}

func (c *strictOpenSBSClient) GetVolumeProfile(_ context.Context, req *GetVolumeProfileRequest) (*GetVolumeProfileResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	return &GetVolumeProfileResponse{VolumeID: req.VolumeID, Profile: profileFromSpec(vol.spec)}, nil
}

func (c *strictOpenSBSClient) GetVolumeStatus(_ context.Context, req *GetVolumeStatusRequest) (*GetVolumeStatusResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	return &GetVolumeStatusResponse{
		VolumeID:       req.VolumeID,
		State:          SBSVolumeStateReady,
		Readable:       true,
		Writable:       true,
		VolumeRevision: vol.revision,
	}, nil
}

func (c *strictOpenSBSClient) Read(_ context.Context, req *ReadRequest) (*ReadResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if vol.handle == "" || vol.attachment != req.Context.AttachmentID || vol.generation != req.Context.Generation {
		return nil, &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "attachment mismatch"}
	}
	start := int(req.OffsetBytes)
	end := start + int(req.LengthBytes)
	return &ReadResponse{
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		Data:           append([]byte(nil), vol.data[start:end]...),
		VolumeRevision: vol.revision,
	}, nil
}

func (c *strictOpenSBSClient) Write(_ context.Context, req *WriteRequest) (*WriteResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if vol.handle == "" || vol.attachment != req.Context.AttachmentID || vol.generation != req.Context.Generation {
		return nil, &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "attachment mismatch"}
	}
	start := int(req.OffsetBytes)
	end := start + int(req.LengthBytes)
	copy(vol.data[start:end], req.Data)
	vol.revision++
	c.volumes[req.VolumeID] = vol
	return &WriteResponse{
		Status:         "ok",
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		CommitID:       "commit",
		VolumeRevision: vol.revision,
	}, nil
}

func (c *strictOpenSBSClient) Flush(_ context.Context, req *FlushRequest) (*FlushResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	return &FlushResponse{Status: "ok", VolumeRevision: vol.revision}, nil
}

func (c *strictOpenSBSClient) Discard(ctx context.Context, req *DiscardRequest) (*DiscardResponse, error) {
	resp, err := c.Zero(ctx, &ZeroRequest{
		VolumeID:    req.VolumeID,
		OffsetBytes: req.OffsetBytes,
		LengthBytes: req.LengthBytes,
		Context:     req.Context,
	})
	if err != nil {
		return nil, err
	}
	return &DiscardResponse{Status: "ok", VolumeRevision: resp.VolumeRevision}, nil
}

func (c *strictOpenSBSClient) Zero(_ context.Context, req *ZeroRequest) (*ZeroResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if vol.handle == "" || vol.attachment != req.Context.AttachmentID || vol.generation != req.Context.Generation {
		return nil, &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "attachment mismatch"}
	}
	start := int(req.OffsetBytes)
	end := start + int(req.LengthBytes)
	clear(vol.data[start:end])
	vol.revision++
	c.volumes[req.VolumeID] = vol
	return &ZeroResponse{Status: "ok", VolumeRevision: vol.revision}, nil
}

func TestSBSDataRepositoryServiceRoundTrip(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:             HexVolumeID(101),
		Name:           "vol-a",
		Prefix:         "vol-a-00000065",
		SizeBytes:      4096 * 8,
		BlockSize:      4096,
		ChunkSizeBytes: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := NewInMemorySBSClient([]VolumeSpec{spec})
	repo := NewSBSDataRepository(meta, sbs, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")

	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	payload := make([]byte, 4096)
	payload[0] = 0xAB
	payload[4095] = 0xCD
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	got, err := svc.Read(context.Background(), 101, 0, uint64(len(payload)))
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(got) != len(payload) || got[0] != payload[0] || got[4095] != payload[4095] {
		t.Fatalf("unexpected read data")
	}

	if err := svc.Flush(context.Background(), 101); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if err := svc.Zero(context.Background(), 101, 0, 4096); err != nil {
		t.Fatalf("Zero failed: %v", err)
	}
	got, err = svc.Read(context.Background(), 101, 0, 4096)
	if err != nil {
		t.Fatalf("Read after zero failed: %v", err)
	}
	if got[0] != 0 || got[4095] != 0 {
		t.Fatalf("expected zeroed data after Zero")
	}

	payload[0] = 0x55
	payload[4095] = 0x66
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("second Write failed: %v", err)
	}
	if err := svc.Discard(context.Background(), 101, 0, 4096); err != nil {
		t.Fatalf("Discard failed: %v", err)
	}
	got, err = svc.Read(context.Background(), 101, 0, 4096)
	if err != nil {
		t.Fatalf("Read after discard failed: %v", err)
	}
	if got[0] != 0 || got[4095] != 0 {
		t.Fatalf("expected zeroed data after Discard")
	}

	metrics := svc.MetricsSnapshot()
	if metrics.IOIdentity == nil {
		t.Fatalf("io_identity metrics missing: %+v", metrics)
	}
	identity := metrics.IOIdentity
	if identity.DiscardBytes != 4096 || identity.LogicalZeroBytes != 8192 {
		t.Fatalf("unexpected zero/discard identity bytes: %+v", identity)
	}
	if identity.DiscardZeroFallbackBytes != 4096 || identity.DiscardTrueReclaimBytes != 0 {
		t.Fatalf("unexpected discard policy bytes: %+v", identity)
	}
	if identity.DiscardAlignmentFallbacks != 0 || identity.DiscardUnalignedCount != 0 || identity.DiscardAlignedCount != 1 {
		t.Fatalf("unexpected discard alignment counters: %+v", identity)
	}
	if identity.ByDiscardPolicy[DiscardPolicyZeroFallback] != 1 {
		t.Fatalf("unexpected discard policy counters: %+v", identity.ByDiscardPolicy)
	}
	if identity.LastObservation == nil || identity.LastObservation.Operation != IOOperationDiscard || identity.LastObservation.Policy != DiscardPolicyZeroFallback {
		t.Fatalf("unexpected last discard observation: %+v", identity.LastObservation)
	}
}

func TestSBSDataRepositoryWriteAtForwardsPayloadWithoutPreRPCCopy(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:             HexVolumeID(101),
		Name:           "vol-a",
		Prefix:         "vol-a-00000065",
		SizeBytes:      4096 * 8,
		BlockSize:      4096,
		ChunkSizeBytes: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	payload := make([]byte, 4096)
	payload[0] = 0xAB
	payload[4095] = 0xCD
	sbs := &writeDataAliasObservingClient{
		serviceSBSClient: newStrictOpenSBSClient([]VolumeSpec{spec}),
		expected:         payload,
	}
	repo := NewSBSDataRepository(meta, sbs, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")

	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if !sbs.aliased {
		t.Fatalf("SBS write request payload was copied before the RPC boundary")
	}
	for i := range payload {
		payload[i] = 0
	}
	got, err := svc.Read(context.Background(), 101, 0, uint64(len(payload)))
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if got[0] != 0xAB || got[4095] != 0xCD {
		t.Fatalf("SBS client retained caller payload after Write returned")
	}
}

func TestServiceDiscardFallsBackForUnalignedFstrimRange(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: uint64(DefaultAllocationChunkSize * 2),
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := NewInMemorySBSClient([]VolumeSpec{spec})
	repo := NewSBSDataRepository(meta, sbs, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")

	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}
	payload := make([]byte, 4096)
	payload[0] = 0x77
	payload[4095] = 0x88
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("Write seed failed: %v", err)
	}
	if err := svc.Discard(context.Background(), 101, 0, 4096); err != nil {
		t.Fatalf("Discard unaligned fallback failed: %v", err)
	}
	got, err := svc.Read(context.Background(), 101, 0, 4096)
	if err != nil {
		t.Fatalf("Read after discard fallback failed: %v", err)
	}
	if got[0] != 0 || got[4095] != 0 {
		t.Fatalf("expected zeroed data after unaligned discard fallback")
	}

	metrics := svc.MetricsSnapshot()
	identity := metrics.IOIdentity
	if identity == nil {
		t.Fatalf("io_identity metrics missing: %+v", metrics)
	}
	if identity.DiscardBytes != 4096 || identity.LogicalZeroBytes != 4096 {
		t.Fatalf("unexpected identity bytes: %+v", identity)
	}
	if identity.DiscardZeroFallbackBytes != 4096 || identity.DiscardTrueReclaimBytes != 0 {
		t.Fatalf("unexpected discard reclaim/fallback bytes: %+v", identity)
	}
	if identity.DiscardUnalignedCount != 1 || identity.DiscardAlignmentFallbacks != 1 {
		t.Fatalf("unexpected discard alignment counters: %+v", identity)
	}
	if identity.ByDiscardPolicy[DiscardPolicyZeroFallback] != 1 {
		t.Fatalf("unexpected discard policy counters: %+v", identity.ByDiscardPolicy)
	}
	if identity.LastObservation == nil || identity.LastObservation.Policy != DiscardPolicyZeroFallback {
		t.Fatalf("discard fallback should remain the last IO identity observation: %+v", identity.LastObservation)
	}
	discardMetrics := metrics.ByOperation["discard"]
	if discardMetrics.Count != 1 || discardMetrics.Errors != 0 {
		t.Fatalf("discard operation metrics=%+v want one successful fallback discard", discardMetrics)
	}
}

func TestSBSDataRepositoryRejectsStaleWriterAfterGenerationBump(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := NewInMemorySBSClient([]VolumeSpec{spec})
	repo := NewSBSDataRepository(meta, sbs, "gw-a").(*sbsDataRepository)
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")

	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	openState, reqCtx, err := repo.ensureOpen(context.Background(), spec)
	if err != nil {
		t.Fatalf("ensureOpen failed: %v", err)
	}
	if _, err := meta.UnsafeSetGeneration(context.Background(), 101, 2); err != nil {
		t.Fatalf("UnsafeSetGeneration failed: %v", err)
	}
	if _, _, err := repo.ensureOpen(context.Background(), spec); err != nil {
		t.Fatalf("ensureOpen after generation bump failed: %v", err)
	}

	_, err = sbs.Write(context.Background(), &WriteRequest{
		VolumeID:     CanonicalVolumeID(101),
		VolumeHandle: openState.handle,
		OffsetBytes:  0,
		LengthBytes:  4096,
		Data:         make([]byte, 4096),
		Context:      reqCtx,
	})
	if err == nil {
		t.Fatalf("expected stale writer rejection")
	}
	var sbsErr *SBSError
	if !errors.As(err, &sbsErr) || sbsErr.Code != SBSErrorCodeStaleGeneration {
		t.Fatalf("expected stale_generation error, got %v", err)
	}
}

func TestSBSDataRepositoryLabOpenReuseTTLReusesValidatedHandle(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := NewInMemorySBSClient([]VolumeSpec{spec})
	repo := NewSBSDataRepositoryWithOpenReuseTTL(meta, sbs, "gw-a", time.Hour).(*sbsDataRepository)
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")

	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	openState, reqCtx, err := repo.ensureOpen(context.Background(), spec)
	if err != nil {
		t.Fatalf("ensureOpen failed: %v", err)
	}
	if reqCtx.Generation != 1 {
		t.Fatalf("initial generation = %d, want 1", reqCtx.Generation)
	}
	if _, err := meta.UnsafeSetGeneration(context.Background(), 101, 2); err != nil {
		t.Fatalf("UnsafeSetGeneration failed: %v", err)
	}
	cachedState, cachedCtx, err := repo.ensureOpen(context.Background(), spec)
	if err != nil {
		t.Fatalf("cached ensureOpen failed: %v", err)
	}
	if cachedCtx.Generation != 1 {
		t.Fatalf("cached generation = %d, want 1", cachedCtx.Generation)
	}
	if cachedState.handle != openState.handle {
		t.Fatalf("cached handle = %q, want %q", cachedState.handle, openState.handle)
	}

	repo.evictHandle(uint64(spec.ID))
	_, refreshedCtx, err := repo.ensureOpen(context.Background(), spec)
	if err != nil {
		t.Fatalf("refreshed ensureOpen failed: %v", err)
	}
	if refreshedCtx.Generation != 2 {
		t.Fatalf("refreshed generation = %d, want 2", refreshedCtx.Generation)
	}
}

func TestSBSDataRepositoryInitialZeroMapEvidenceUsesAllocationResolver(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:              HexVolumeID(101),
		Name:            "vol-a",
		SizeBytes:       4 * 4096,
		BlockSize:       4096,
		ChunkSizeBytes:  4096,
		ExtentPageBytes: 4 * 4096,
	})
	resolver := fakeInitialZeroMapResolver{pages: []clustermeta.ResolvedAllocationPage{{
		Page: clustermeta.AllocationPageRecord{
			VolumeID:       CanonicalVolumeID(uint64(spec.ID)),
			PageNo:         0,
			PageBytes:      spec.ExtentPageBytes,
			ChunkSizeBytes: spec.ChunkSizeBytes,
			Extents: []clustermeta.AllocationExtentRecord{{
				LogicalChunkStart: 0,
				ChunkCount:        4,
				Kind:              clustermeta.AllocationKindZero,
			}},
		},
		RangeStartChunk: 0,
		RangeEndChunk:   4,
		CoversWholePage: true,
	}}}
	repo := NewSBSDataRepositoryWithOpenReuseTTLAndAllocationResolver(nil, nil, "gw-a", 0, resolver).(InitialZeroMapEvidenceProvider)

	evidence, err := repo.InitialZeroMapEvidence(context.Background(), spec)
	if err != nil {
		t.Fatalf("InitialZeroMapEvidence: %v", err)
	}
	if !evidence.Trusted || !evidence.AllZero || evidence.CheckedPageCount != 1 || evidence.CheckedChunkCount != 4 {
		t.Fatalf("unexpected evidence: %+v", evidence)
	}
}

func TestSBSDataRepositoryInitialZeroMapEvidenceRejectsDataExtent(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:              HexVolumeID(101),
		Name:            "vol-a",
		SizeBytes:       4 * 4096,
		BlockSize:       4096,
		ChunkSizeBytes:  4096,
		ExtentPageBytes: 4 * 4096,
	})
	resolver := fakeInitialZeroMapResolver{pages: []clustermeta.ResolvedAllocationPage{{
		Page: clustermeta.AllocationPageRecord{
			VolumeID:       CanonicalVolumeID(uint64(spec.ID)),
			PageNo:         0,
			PageBytes:      spec.ExtentPageBytes,
			ChunkSizeBytes: spec.ChunkSizeBytes,
			Extents: []clustermeta.AllocationExtentRecord{{
				LogicalChunkStart:  0,
				ChunkCount:         1,
				Kind:               clustermeta.AllocationKindData,
				PhysicalChunkStart: 100,
			}, {
				LogicalChunkStart: 1,
				ChunkCount:        3,
				Kind:              clustermeta.AllocationKindZero,
			}},
		},
		RangeStartChunk: 0,
		RangeEndChunk:   4,
		CoversWholePage: true,
	}}}
	repo := NewSBSDataRepositoryWithOpenReuseTTLAndAllocationResolver(nil, nil, "gw-a", 0, resolver).(InitialZeroMapEvidenceProvider)

	evidence, err := repo.InitialZeroMapEvidence(context.Background(), spec)
	if err != nil {
		t.Fatalf("InitialZeroMapEvidence: %v", err)
	}
	if !evidence.Trusted || evidence.AllZero {
		t.Fatalf("unexpected evidence: %+v", evidence)
	}
}

func TestSBSDataRepositoryRejectsWriterFencedGatewayAfterHandoffRequired(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := NewInMemorySBSClient([]VolumeSpec{spec})
	repo := NewSBSDataRepository(meta, sbs, "gw-a").(*sbsDataRepository)
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")

	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	mem := meta.(*inMemoryMetadataRepository)
	mem.mu.Lock()
	status := mem.status[101]
	status.HandoffRequired = true
	status.HandoffReason = "current_gateway_not_desired"
	status.HandoffTargetGatewaySet = []string{"gw-b"}
	status.WriterFencingEpoch = 9
	mem.status[101] = status
	mem.mu.Unlock()

	_, _, err := repo.ensureOpen(context.Background(), spec)
	if err == nil {
		t.Fatalf("expected writer fenced error")
	}
	if !errors.Is(err, ErrWriterFenced) {
		t.Fatalf("expected ErrWriterFenced, got %v", err)
	}
}

func TestSBSDataRepositoryEmitsStructuredLogs(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := NewInMemorySBSClient([]VolumeSpec{spec})
	repo := NewSBSDataRepository(meta, sbs, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")

	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()

	payload := make([]byte, 4096)
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	logs := buf.String()
	for _, want := range []string{
		`"component":"gateway.sbs"`,
		`"event":"sbs_write_completed"`,
		`"request_id":"sbs-req-1"`,
		`"trace_id":"trace-1"`,
		`"volume_id":"00000065"`,
		`"idempotency_key":"sbs-idem-`,
		`att-00000065-0001`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs missing %q: %s", want, logs)
		}
	}
}

func TestSBSDataRepositoryReopensAfterGenerationChange(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	localClient := newStrictOpenSBSClient([]VolumeSpec{spec})
	repo := NewSBSDataRepository(meta, localClient, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")

	st, err := svc.Attach(101, "host-a", 1)
	if err != nil {
		t.Fatalf("Attach failed: %v", err)
	}
	if st.AttachmentID != "att-00000065-0001" {
		t.Fatalf("unexpected initial attachment: %+v", st)
	}

	payloadA := make([]byte, 4096)
	copy(payloadA, []byte("before-reattach"))
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payloadA)), payloadA); err != nil {
		t.Fatalf("Write before detach failed: %v", err)
	}

	if _, err := svc.Detach(101, "host-a", "att-00000065-0001"); err != nil {
		t.Fatalf("Detach failed: %v", err)
	}
	st, err = svc.Attach(101, "host-a", 2)
	if err != nil {
		t.Fatalf("Reattach failed: %v", err)
	}
	if st.AttachmentID != "att-00000065-0002" || st.Generation != 2 {
		t.Fatalf("unexpected reattach state: %+v", st)
	}

	got, err := svc.Read(context.Background(), 101, 0, uint64(len(payloadA)))
	if err != nil {
		t.Fatalf("Read after reattach failed: %v", err)
	}
	if string(got[:len("before-reattach")]) != "before-reattach" {
		t.Fatalf("unexpected payload after reattach: %q", got[:len("before-reattach")])
	}
}

func TestSBSDataRepositoryDetachClosesWriterContextForGatewayHandoff(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	localClient := newStrictOpenSBSClient([]VolumeSpec{spec})
	primaryRepo := NewSBSDataRepository(meta, localClient, "gw-a")
	secondaryRepo := NewSBSDataRepository(meta, localClient, "gw-b")
	primarySvc := NewWithRepositoryOptions(meta, primaryRepo, "gw-a")
	secondarySvc := NewWithRepositoryOptions(meta, secondaryRepo, "gw-b")

	st, err := primarySvc.Attach(101, "host-a", 1)
	if err != nil {
		t.Fatalf("primary Attach failed: %v", err)
	}
	payload := make([]byte, 4096)
	copy(payload, []byte("handoff-seed"))
	if err := primarySvc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("primary Write failed: %v", err)
	}

	if _, err := primarySvc.Detach(101, "host-a", st.AttachmentID); err != nil {
		t.Fatalf("primary Detach failed: %v", err)
	}
	st, err = secondarySvc.Attach(101, "host-b", 2)
	if err != nil {
		t.Fatalf("secondary Attach failed: %v", err)
	}
	if st.Generation == 0 || st.AttachmentID == "" {
		t.Fatalf("unexpected secondary attachment state: %+v", st)
	}

	got, err := secondarySvc.Read(context.Background(), 101, 0, uint64(len(payload)))
	if err != nil {
		t.Fatalf("secondary Read after handoff failed: %v", err)
	}
	if string(got[:len("handoff-seed")]) != "handoff-seed" {
		t.Fatalf("unexpected payload after gateway handoff: %q", got[:len("handoff-seed")])
	}
}

func TestSBSDataRepositoryRetriesOpenAfterRetryableUnavailable(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := newUnavailableOnceSBSClient(newStrictOpenSBSClient([]VolumeSpec{spec}))
	repo := NewSBSDataRepository(meta, sbs, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")

	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	payload := make([]byte, 4096)
	copy(payload, []byte("retry-open"))
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("Write after retryable unavailable failed: %v", err)
	}
	got, err := svc.Read(context.Background(), 101, 0, uint64(len(payload)))
	if err != nil {
		t.Fatalf("Read after retryable unavailable failed: %v", err)
	}
	if string(got[:len("retry-open")]) != "retry-open" {
		t.Fatalf("unexpected payload after retryable unavailable: %q", got[:len("retry-open")])
	}
}

func TestSBSDataRepositoryRetriesWriteAfterAttachmentMismatch(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := newFailOnceIOClient(newStrictOpenSBSClient([]VolumeSpec{spec}), false, true, false)
	repo := NewSBSDataRepository(meta, sbs, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")
	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	payload := make([]byte, 4096)
	copy(payload, []byte("retry-write"))
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("Write after attachment mismatch failed: %v", err)
	}
	got, err := svc.Read(context.Background(), 101, 0, uint64(len(payload)))
	if err != nil {
		t.Fatalf("Read after retried write failed: %v", err)
	}
	if string(got[:len("retry-write")]) != "retry-write" {
		t.Fatalf("unexpected payload after retried write: %q", got[:len("retry-write")])
	}
}

func TestSBSDataRepositoryRetriesReadAfterAttachmentMismatch(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := newFailOnceIOClient(newStrictOpenSBSClient([]VolumeSpec{spec}), true, false, false)
	repo := NewSBSDataRepository(meta, sbs, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")
	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	payload := make([]byte, 4096)
	copy(payload, []byte("retry-read"))
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("seed Write failed: %v", err)
	}
	got, err := svc.Read(context.Background(), 101, 0, uint64(len(payload)))
	if err != nil {
		t.Fatalf("Read after attachment mismatch failed: %v", err)
	}
	if string(got[:len("retry-read")]) != "retry-read" {
		t.Fatalf("unexpected payload after retried read: %q", got[:len("retry-read")])
	}
}

func TestSBSDataRepositoryRetriesFlushAfterAttachmentMismatch(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := newFailOnceIOClient(newStrictOpenSBSClient([]VolumeSpec{spec}), false, false, true)
	repo := NewSBSDataRepository(meta, sbs, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")
	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	payload := make([]byte, 4096)
	copy(payload, []byte("retry-flush"))
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("seed Write failed: %v", err)
	}
	if err := svc.Flush(context.Background(), 101); err != nil {
		t.Fatalf("Flush after attachment mismatch failed: %v", err)
	}
}

func TestSBSDataRepositoryRetriesDiscardAfterAttachmentMismatch(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:             HexVolumeID(101),
		Name:           "vol-a",
		Prefix:         "vol-a-00000065",
		SizeBytes:      4096 * 8,
		BlockSize:      4096,
		ChunkSizeBytes: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := newFailOnceIOClient(newStrictOpenSBSClient([]VolumeSpec{spec}), false, true, false)
	repo := NewSBSDataRepository(meta, sbs, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")
	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	payload := make([]byte, 4096)
	copy(payload, []byte("retry-discard"))
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("seed Write failed: %v", err)
	}
	if err := svc.Discard(context.Background(), 101, 0, 4096); err != nil {
		t.Fatalf("Discard after attachment mismatch failed: %v", err)
	}
	got, err := svc.Read(context.Background(), 101, 0, 4096)
	if err != nil {
		t.Fatalf("Read after retried discard failed: %v", err)
	}
	if got[0] != 0 || got[4095] != 0 {
		t.Fatalf("expected zeroed data after retried discard")
	}
}

func TestSBSDataRepositoryRetriesZeroAfterAttachmentMismatch(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := newFailOnceIOClient(newStrictOpenSBSClient([]VolumeSpec{spec}), false, true, false)
	repo := NewSBSDataRepository(meta, sbs, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")
	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	payload := make([]byte, 4096)
	copy(payload, []byte("retry-zero"))
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("seed Write failed: %v", err)
	}
	if err := svc.Zero(context.Background(), 101, 0, 4096); err != nil {
		t.Fatalf("Zero after attachment mismatch failed: %v", err)
	}
	got, err := svc.Read(context.Background(), 101, 0, 4096)
	if err != nil {
		t.Fatalf("Read after retried zero failed: %v", err)
	}
	if got[0] != 0 || got[4095] != 0 {
		t.Fatalf("expected zeroed data after retried zero")
	}
}

func TestSBSDataRepositoryExposesRetryMetricsSnapshot(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := NewInMemoryMetadataRepository([]VolumeSpec{spec})
	sbs := newFailOnceIOClient(newStrictOpenSBSClient([]VolumeSpec{spec}), true, true, true)
	repo := NewSBSDataRepository(meta, sbs, "gw-a")
	svc := NewWithRepositoryOptions(meta, repo, "gw-a")
	if _, err := svc.Attach(101, "host-a", 1); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	payload := make([]byte, 4096)
	copy(payload, []byte("retry-metrics"))
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if _, err := svc.Read(context.Background(), 101, 0, uint64(len(payload))); err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if err := svc.Flush(context.Background(), 101); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	metrics := svc.MetricsSnapshot()
	if metrics.Retry["write_reopen_retry"] != 1 {
		t.Fatalf("write retry metric=%d want=1", metrics.Retry["write_reopen_retry"])
	}
	if metrics.Retry["read_reopen_retry"] != 1 {
		t.Fatalf("read retry metric=%d want=1", metrics.Retry["read_reopen_retry"])
	}
	if metrics.Retry["flush_reopen_retry"] != 1 {
		t.Fatalf("flush retry metric=%d want=1", metrics.Retry["flush_reopen_retry"])
	}
	if metrics.RetrySummary.TotalRetries != 3 || metrics.RetrySummary.OpenUnavailableRetries != 0 || metrics.RetrySummary.ReopenRetries != 3 {
		t.Fatalf("unexpected retry summary: %+v", metrics.RetrySummary)
	}
}
