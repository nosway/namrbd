package service

import "fmt"

const (
	IOOperationDiscard = "discard"
	IOOperationZero    = "zero"

	DiscardPolicyTrueReclaim   = "true_reclaim"
	DiscardPolicyPartialReject = "partial_reject"
	DiscardPolicyZeroFallback  = "zero_fallback"
	DiscardPolicyZero          = "zero"

	DiscardFallbackTrueReclaimNotImplemented = "true_reclaim_not_implemented"
	DiscardFallbackNotAlignedToReclaim       = "not_aligned_to_reclaim_geometry"
)

type DiscardObservation struct {
	Operation                string `json:"operation"`
	BackendType              string `json:"backend_type"`
	ReclaimGeometryBytes     uint64 `json:"reclaim_geometry_bytes"`
	RequestOffsetBytes       uint64 `json:"request_offset_bytes"`
	RequestLengthBytes       uint64 `json:"request_length_bytes"`
	AlignedToReclaimGeometry bool   `json:"aligned_to_reclaim_geometry"`
	Policy                   string `json:"policy"`
	FallbackReason           string `json:"fallback_reason,omitempty"`
	DiscardBytes             uint64 `json:"discard_bytes,omitempty"`
	LogicalZeroBytes         uint64 `json:"logical_zero_bytes,omitempty"`
}

func NewZeroObservation(volume VolumeSpec, offsetBytes, lengthBytes uint64) DiscardObservation {
	return newDiscardObservation(volume, IOOperationZero, offsetBytes, lengthBytes, DiscardPolicyZero, "")
}

func NewDiscardZeroFallbackObservation(volume VolumeSpec, offsetBytes, lengthBytes uint64) DiscardObservation {
	obs := newDiscardObservation(volume, IOOperationDiscard, offsetBytes, lengthBytes, DiscardPolicyZeroFallback, DiscardFallbackTrueReclaimNotImplemented)
	if !obs.AlignedToReclaimGeometry {
		obs.FallbackReason = DiscardFallbackNotAlignedToReclaim
	}
	return obs
}

func NewDiscardTrueReclaimObservation(volume VolumeSpec, offsetBytes, lengthBytes uint64) DiscardObservation {
	return newDiscardObservation(volume, IOOperationDiscard, offsetBytes, lengthBytes, DiscardPolicyTrueReclaim, "")
}

func NewDiscardPartialRejectObservation(volume VolumeSpec, offsetBytes, lengthBytes uint64) DiscardObservation {
	return newDiscardObservation(volume, IOOperationDiscard, offsetBytes, lengthBytes, DiscardPolicyPartialReject, DiscardFallbackNotAlignedToReclaim)
}

func NewDiscardAlignmentZeroFallbackObservation(volume VolumeSpec, offsetBytes, lengthBytes uint64) DiscardObservation {
	return newDiscardObservation(volume, IOOperationDiscard, offsetBytes, lengthBytes, DiscardPolicyZeroFallback, DiscardFallbackNotAlignedToReclaim)
}

func NewDiscardAlignmentError(volume VolumeSpec, offsetBytes, lengthBytes uint64) error {
	geometry := reclaimGeometryBytes(volume)
	return fmt.Errorf("%w %d bytes (offset_bytes=%d length_bytes=%d)", ErrDiscardAlignment, geometry, offsetBytes, lengthBytes)
}

func newDiscardObservation(volume VolumeSpec, operation string, offsetBytes, lengthBytes uint64, policy, fallbackReason string) DiscardObservation {
	volume = NormalizeVolumeSpec(volume)
	geometry := reclaimGeometryBytes(volume)
	aligned := geometry > 0 && offsetBytes%geometry == 0 && lengthBytes%geometry == 0
	obs := DiscardObservation{
		Operation:                operation,
		BackendType:              volume.RedundancyBackend,
		ReclaimGeometryBytes:     geometry,
		RequestOffsetBytes:       offsetBytes,
		RequestLengthBytes:       lengthBytes,
		AlignedToReclaimGeometry: aligned,
		Policy:                   policy,
		FallbackReason:           fallbackReason,
	}
	switch operation {
	case IOOperationDiscard:
		obs.DiscardBytes = lengthBytes
		if policy == DiscardPolicyZeroFallback || policy == DiscardPolicyTrueReclaim {
			obs.LogicalZeroBytes = lengthBytes
		}
	case IOOperationZero:
		obs.LogicalZeroBytes = lengthBytes
	}
	return obs
}

func reclaimGeometryBytes(volume VolumeSpec) uint64 {
	volume = NormalizeVolumeSpec(volume)
	if volume.RedundancyBackend == RedundancyBackendEC {
		if volume.ECDataShards > 0 && volume.ECStripeUnitBytes > 0 {
			return uint64(volume.ECDataShards) * uint64(volume.ECStripeUnitBytes)
		}
		return uint64(volume.ExtentPageBytes)
	}
	return uint64(volume.ChunkSizeBytes)
}

func reclaimAligned(volume VolumeSpec, offsetBytes, lengthBytes uint64) bool {
	geometry := reclaimGeometryBytes(volume)
	return geometry > 0 && offsetBytes%geometry == 0 && lengthBytes%geometry == 0
}

func DiscardRangeAligned(volume VolumeSpec, offsetBytes, lengthBytes uint64) bool {
	return reclaimAligned(volume, offsetBytes, lengthBytes)
}
