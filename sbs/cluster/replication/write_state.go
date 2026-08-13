package replication

import (
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type WritePipelineState string

const (
	WriteStateReceived          WritePipelineState = "received"
	WriteStateValidated         WritePipelineState = "validated"
	WriteStateIntentPending     WritePipelineState = "intent_pending"
	WriteStatePayloadQuorumDone WritePipelineState = "payload_quorum_done"
	WriteStateMetadataCommitted WritePipelineState = "metadata_committed"
	WriteStateAcked             WritePipelineState = "acked"
	WriteStateFailed            WritePipelineState = "failed"
)

type ExtentWriteStatus struct {
	Plan                   ExtentWritePlan
	AckedReplicaIDs        map[string]struct{}
	ChunkEncryptionHeaders map[uint64]*metadata.PayloadEncryptionHeader
	PrimaryAcked           bool
	QuorumReached          bool
	Failed                 bool
	LastError              string
}

type WriteExecution struct {
	VolumeID                string
	RequestID               string
	AttachmentID            string
	Generation              uint64
	IdempotencyKey          string
	MetadataEpoch           uint64
	MetadataRevision        uint64
	MutationOperation       metadata.MutationOperationRecord
	AllowMissingWriteIntent bool
	State                   WritePipelineState
	Extents                 []ExtentWriteStatus
	LastError               string
}

func NewWriteExecution(plan *WritePlan, requestID, attachmentID string, generation uint64, idempotencyKey string, metadataEpoch, metadataRevision uint64) *WriteExecution {
	extents := make([]ExtentWriteStatus, 0, len(plan.Extents))
	for _, extent := range plan.Extents {
		extents = append(extents, ExtentWriteStatus{
			Plan:                   extent,
			AckedReplicaIDs:        make(map[string]struct{}),
			ChunkEncryptionHeaders: make(map[uint64]*metadata.PayloadEncryptionHeader),
		})
	}
	return &WriteExecution{
		VolumeID:         plan.VolumeID,
		RequestID:        requestID,
		AttachmentID:     attachmentID,
		Generation:       generation,
		IdempotencyKey:   idempotencyKey,
		MetadataEpoch:    metadataEpoch,
		MetadataRevision: metadataRevision,
		State:            WriteStateReceived,
		Extents:          extents,
	}
}

func (w *WriteExecution) MarkValidated() {
	if w.State == WriteStateReceived {
		w.State = WriteStateValidated
	}
}

func (w *WriteExecution) MarkIntentPending() {
	if w.State == WriteStateValidated || w.State == WriteStateReceived {
		w.State = WriteStateIntentPending
	}
}

func (w *WriteExecution) RecordChunkEncryptionHeaders(extentIndex int, headers []ReplicaChunkEncryptionHeader) error {
	if len(headers) == 0 {
		return nil
	}
	if extentIndex < 0 || extentIndex >= len(w.Extents) {
		return fmt.Errorf("extent index %d out of range", extentIndex)
	}
	status := &w.Extents[extentIndex]
	if status.ChunkEncryptionHeaders == nil {
		status.ChunkEncryptionHeaders = make(map[uint64]*metadata.PayloadEncryptionHeader)
	}
	for _, header := range headers {
		if header.Header == nil {
			continue
		}
		cloned := *header.Header
		status.ChunkEncryptionHeaders[header.LogicalChunk] = &cloned
	}
	return nil
}

func (w *WriteExecution) MarkReplicaAck(extentIndex int, replicaID string) error {
	return w.MarkReplicaAckWithPolicy(extentIndex, replicaID, true)
}

func (w *WriteExecution) MarkReplicaAckWithPolicy(extentIndex int, replicaID string, requirePrimaryAck bool) error {
	if extentIndex < 0 || extentIndex >= len(w.Extents) {
		return fmt.Errorf("extent index %d out of range", extentIndex)
	}
	status := &w.Extents[extentIndex]
	if status.Failed {
		return fmt.Errorf("extent %d already failed", extentIndex)
	}
	status.AckedReplicaIDs[replicaID] = struct{}{}
	if replicaID == status.Plan.Primary.ReplicaID {
		status.PrimaryAcked = true
	}
	if (!requirePrimaryAck || status.PrimaryAcked) && uint32(len(status.AckedReplicaIDs)) >= status.Plan.RequiredAcks {
		status.QuorumReached = true
	}
	if w.AllExtentsQuorumReached() {
		w.State = WriteStatePayloadQuorumDone
	}
	return nil
}

func (w *WriteExecution) MarkExtentFailed(extentIndex int, err error) error {
	if extentIndex < 0 || extentIndex >= len(w.Extents) {
		return fmt.Errorf("extent index %d out of range", extentIndex)
	}
	status := &w.Extents[extentIndex]
	status.Failed = true
	if err != nil {
		status.LastError = err.Error()
		w.LastError = err.Error()
	}
	w.State = WriteStateFailed
	return nil
}

func (w *WriteExecution) AllExtentsQuorumReached() bool {
	if len(w.Extents) == 0 {
		return false
	}
	for _, extent := range w.Extents {
		if !extent.QuorumReached {
			return false
		}
	}
	return true
}

func (w *WriteExecution) CanCommitMetadata() bool {
	return w.State == WriteStatePayloadQuorumDone && w.AllExtentsQuorumReached()
}

func (w *WriteExecution) MarkMetadataCommitted() error {
	if !w.CanCommitMetadata() {
		return fmt.Errorf("write execution is not ready for metadata commit")
	}
	w.State = WriteStateMetadataCommitted
	return nil
}

func (w *WriteExecution) MarkAcked() error {
	if w.State != WriteStateMetadataCommitted {
		return fmt.Errorf("write execution must be metadata committed before ack")
	}
	w.State = WriteStateAcked
	return nil
}

func (w *WriteExecution) MarkFailed(err error) {
	if err != nil {
		w.LastError = err.Error()
	}
	w.State = WriteStateFailed
}
