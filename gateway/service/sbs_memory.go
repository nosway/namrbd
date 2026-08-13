package service

import (
	"context"
	"fmt"
	"sync"

	namrbdversion "github.com/nosway/namrbd/version"
)

type inMemorySBSVolume struct {
	spec       VolumeSpec
	data       []byte
	revision   uint64
	state      SBSVolumeState
	handle     string
	attachment string
	generation uint64
}

type InMemorySBSClient struct {
	mu      sync.Mutex
	volumes map[string]*inMemorySBSVolume
}

func NewInMemorySBSClient(volumes []VolumeSpec) *InMemorySBSClient {
	client := &InMemorySBSClient{
		volumes: make(map[string]*inMemorySBSVolume, len(volumes)),
	}
	for _, spec := range volumes {
		spec = NormalizeVolumeSpec(spec)
		id := CanonicalVolumeID(uint64(spec.ID))
		client.volumes[id] = &inMemorySBSVolume{
			spec:     spec,
			data:     make([]byte, spec.SizeBytes),
			revision: 1,
			state:    SBSVolumeStateReady,
		}
	}
	return client
}

func (c *InMemorySBSClient) OpenVolume(_ context.Context, req *OpenVolumeRequest) (*OpenVolumeResponse, error) {
	if req == nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: "nil request"}
	}
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if vol.state == SBSVolumeStateUnavailable {
		return nil, &SBSError{Code: SBSErrorCodeUnavailable, Message: "volume unavailable", Retryable: true}
	}
	vol.attachment = req.Context.AttachmentID
	vol.generation = req.Context.Generation
	vol.handle = fmt.Sprintf("vh-%s-%s", req.VolumeID, req.Context.AttachmentID)
	return &OpenVolumeResponse{
		Status:         "ok",
		VolumeHandle:   vol.handle,
		VolumeID:       req.VolumeID,
		VolumeRevision: vol.revision,
		Profile:        profileFromSpec(vol.spec),
		ServerVersion:  namrbdversion.Current,
	}, nil
}

func (c *InMemorySBSClient) CloseVolume(_ context.Context, req *CloseVolumeRequest) (*CloseVolumeResponse, error) {
	if req == nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: "nil request"}
	}
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if err := validateWriterContext(vol, req.Context); err != nil {
		return nil, err
	}
	vol.handle = ""
	return &CloseVolumeResponse{Status: "ok"}, nil
}

func (c *InMemorySBSClient) GetVolumeProfile(_ context.Context, req *GetVolumeProfileRequest) (*GetVolumeProfileResponse, error) {
	if req == nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: "nil request"}
	}
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	return &GetVolumeProfileResponse{
		VolumeID: req.VolumeID,
		Profile:  profileFromSpec(vol.spec),
	}, nil
}

func (c *InMemorySBSClient) GetVolumeStatus(_ context.Context, req *GetVolumeStatusRequest) (*GetVolumeStatusResponse, error) {
	if req == nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: "nil request"}
	}
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	return &GetVolumeStatusResponse{
		VolumeID:       req.VolumeID,
		State:          vol.state,
		Readable:       vol.state != SBSVolumeStateUnavailable,
		Writable:       vol.state == SBSVolumeStateReady || vol.state == SBSVolumeStateDegraded,
		VolumeRevision: vol.revision,
	}, nil
}

func (c *InMemorySBSClient) Read(_ context.Context, req *ReadRequest) (*ReadResponse, error) {
	if req == nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: "nil request"}
	}
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if err := validateWriterContext(vol, req.Context); err != nil {
		return nil, err
	}
	if err := validateSBSRange(vol.spec, req.OffsetBytes, req.LengthBytes); err != nil {
		return nil, err
	}
	start := int(req.OffsetBytes)
	end := start + int(req.LengthBytes)
	data := append([]byte(nil), vol.data[start:end]...)
	return &ReadResponse{
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		Data:           data,
		VolumeRevision: vol.revision,
	}, nil
}

func (c *InMemorySBSClient) Write(_ context.Context, req *WriteRequest) (*WriteResponse, error) {
	if req == nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: "nil request"}
	}
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if err := validateWriterContext(vol, req.Context); err != nil {
		return nil, err
	}
	if err := validateSBSRange(vol.spec, req.OffsetBytes, req.LengthBytes); err != nil {
		return nil, err
	}
	start := int(req.OffsetBytes)
	end := start + int(req.LengthBytes)
	copy(vol.data[start:end], req.Data)
	vol.revision++
	return &WriteResponse{
		Status:         "ok",
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		CommitID:       fmt.Sprintf("commit-%s-%d", req.VolumeID, vol.revision),
		VolumeRevision: vol.revision,
	}, nil
}

func (c *InMemorySBSClient) ReadPhysicalChunk(_ context.Context, req *ReadPhysicalChunkRequest) (*ReadPhysicalChunkResponse, error) {
	if req == nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: "nil request"}
	}
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if err := validateWriterContext(vol, req.Context); err != nil {
		return nil, err
	}
	start, end, err := physicalChunkWindow(vol.spec, req.PhysicalChunkID, req.ChunkOffsetBytes, req.LengthBytes)
	if err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}
	if end > len(vol.data) {
		out := make([]byte, req.LengthBytes)
		if start < len(vol.data) {
			copy(out, vol.data[start:])
		}
		return &ReadPhysicalChunkResponse{
			VolumeID:         req.VolumeID,
			PhysicalChunkID:  req.PhysicalChunkID,
			ChunkOffsetBytes: req.ChunkOffsetBytes,
			LengthBytes:      req.LengthBytes,
			Data:             out,
			VolumeRevision:   vol.revision,
		}, nil
	}
	data := append([]byte(nil), vol.data[start:end]...)
	return &ReadPhysicalChunkResponse{
		VolumeID:         req.VolumeID,
		PhysicalChunkID:  req.PhysicalChunkID,
		ChunkOffsetBytes: req.ChunkOffsetBytes,
		LengthBytes:      req.LengthBytes,
		Data:             data,
		VolumeRevision:   vol.revision,
	}, nil
}

func (c *InMemorySBSClient) WritePhysicalChunk(_ context.Context, req *WritePhysicalChunkRequest) (*WritePhysicalChunkResponse, error) {
	if req == nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: "nil request"}
	}
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if err := validateWriterContext(vol, req.Context); err != nil {
		return nil, err
	}
	start, end, err := physicalChunkWindow(vol.spec, req.PhysicalChunkID, req.ChunkOffsetBytes, req.LengthBytes)
	if err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}
	if end > len(vol.data) {
		expanded := make([]byte, end)
		copy(expanded, vol.data)
		vol.data = expanded
	}
	copy(vol.data[start:end], req.Data)
	vol.revision++
	return &WritePhysicalChunkResponse{
		Status:           "ok",
		VolumeID:         req.VolumeID,
		PhysicalChunkID:  req.PhysicalChunkID,
		ChunkOffsetBytes: req.ChunkOffsetBytes,
		LengthBytes:      req.LengthBytes,
		CommitID:         fmt.Sprintf("physical-commit-%s-%d", req.VolumeID, vol.revision),
		VolumeRevision:   vol.revision,
	}, nil
}

func (c *InMemorySBSClient) Flush(_ context.Context, req *FlushRequest) (*FlushResponse, error) {
	if req == nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: "nil request"}
	}
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if err := validateWriterContext(vol, req.Context); err != nil {
		return nil, err
	}
	return &FlushResponse{Status: "ok", VolumeRevision: vol.revision}, nil
}

func (c *InMemorySBSClient) Discard(_ context.Context, req *DiscardRequest) (*DiscardResponse, error) {
	if req == nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: "nil request"}
	}
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if err := validateWriterContext(vol, req.Context); err != nil {
		return nil, err
	}
	if err := validateSBSRange(vol.spec, req.OffsetBytes, req.LengthBytes); err != nil {
		return nil, err
	}
	start := int(req.OffsetBytes)
	end := start + int(req.LengthBytes)
	clear(vol.data[start:end])
	vol.revision++
	return &DiscardResponse{Status: "ok", VolumeRevision: vol.revision}, nil
}

func (c *InMemorySBSClient) Zero(_ context.Context, req *ZeroRequest) (*ZeroResponse, error) {
	if req == nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: "nil request"}
	}
	if err := req.Validate(); err != nil {
		return nil, &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	vol, ok := c.volumes[req.VolumeID]
	if !ok {
		return nil, &SBSError{Code: SBSErrorCodeNotFound, Message: "volume not found"}
	}
	if err := validateWriterContext(vol, req.Context); err != nil {
		return nil, err
	}
	if err := validateSBSRange(vol.spec, req.OffsetBytes, req.LengthBytes); err != nil {
		return nil, err
	}
	start := int(req.OffsetBytes)
	end := start + int(req.LengthBytes)
	clear(vol.data[start:end])
	vol.revision++
	return &ZeroResponse{Status: "ok", VolumeRevision: vol.revision}, nil
}

func profileFromSpec(spec VolumeSpec) SBSVolumeProfile {
	return SBSVolumeProfile{
		SizeBytes:       spec.SizeBytes,
		BlockSize:       spec.BlockSize,
		MaxIOSize:       spec.ExtentPageBytes,
		SupportsFlush:   true,
		SupportsDiscard: true,
		SupportsZero:    true,
		ConsistencyMode: "single-writer-linearized",
	}
}

func validateWriterContext(vol *inMemorySBSVolume, ctx SBSRequestContext) error {
	if vol.attachment == "" {
		return &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "volume is not opened for a writer"}
	}
	if ctx.AttachmentID != vol.attachment {
		return &SBSError{Code: SBSErrorCodeAttachmentMismatch, Message: "attachment mismatch"}
	}
	if ctx.Generation != vol.generation {
		return &SBSError{Code: SBSErrorCodeStaleGeneration, Message: "generation mismatch"}
	}
	return nil
}

func validateSBSRange(spec VolumeSpec, offsetBytes, lengthBytes uint64) error {
	if err := validateRange(spec.BlockSize, spec.SizeBytes, offsetBytes, lengthBytes); err != nil {
		switch err {
		case ErrBadAlignment, ErrBadDataLength:
			return &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
		case ErrOutOfRange:
			return &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
		default:
			return &SBSError{Code: SBSErrorCodeBadRequest, Message: err.Error()}
		}
	}
	return nil
}

func physicalChunkWindow(spec VolumeSpec, physicalChunkID, chunkOffsetBytes, lengthBytes uint64) (int, int, error) {
	chunkSize := uint64(spec.ChunkSizeBytes)
	if chunkSize == 0 {
		chunkSize = DefaultAllocationChunkSize
	}
	if chunkOffsetBytes >= chunkSize || chunkOffsetBytes+lengthBytes > chunkSize {
		return 0, 0, ErrOutOfRange
	}
	if spec.BlockSize == 0 {
		spec.BlockSize = DefaultBlockSize
	}
	if chunkOffsetBytes%uint64(spec.BlockSize) != 0 || lengthBytes%uint64(spec.BlockSize) != 0 {
		return 0, 0, ErrBadAlignment
	}
	start := physicalChunkID*chunkSize + chunkOffsetBytes
	end := start + lengthBytes
	if end > uint64(^uint(0)>>1) {
		return 0, 0, ErrOutOfRange
	}
	return int(start), int(end), nil
}
