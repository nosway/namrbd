package service

import "testing"

func TestDiscardObservationReplicatedGeometryAndFallback(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:             HexVolumeID(101),
		SizeBytes:      1 << 30,
		ChunkSizeBytes: 64 << 10,
	})

	aligned := NewDiscardZeroFallbackObservation(spec, 128<<10, 64<<10)
	if aligned.Operation != IOOperationDiscard {
		t.Fatalf("operation=%q want discard", aligned.Operation)
	}
	if aligned.BackendType != RedundancyBackendReplicated {
		t.Fatalf("backend_type=%q want replicated", aligned.BackendType)
	}
	if aligned.ReclaimGeometryBytes != 64<<10 {
		t.Fatalf("reclaim_geometry_bytes=%d want %d", aligned.ReclaimGeometryBytes, 64<<10)
	}
	if !aligned.AlignedToReclaimGeometry {
		t.Fatalf("expected aligned observation: %+v", aligned)
	}
	if aligned.Policy != DiscardPolicyZeroFallback || aligned.FallbackReason != DiscardFallbackTrueReclaimNotImplemented {
		t.Fatalf("unexpected aligned fallback policy: %+v", aligned)
	}
	if aligned.DiscardBytes != 64<<10 || aligned.LogicalZeroBytes != 64<<10 {
		t.Fatalf("unexpected byte counters: %+v", aligned)
	}

	unaligned := NewDiscardAlignmentZeroFallbackObservation(spec, 0, 4096)
	if unaligned.AlignedToReclaimGeometry {
		t.Fatalf("expected unaligned observation: %+v", unaligned)
	}
	if unaligned.Policy != DiscardPolicyZeroFallback {
		t.Fatalf("policy=%q want zero_fallback", unaligned.Policy)
	}
	if unaligned.FallbackReason != DiscardFallbackNotAlignedToReclaim {
		t.Fatalf("fallback_reason=%q want %q", unaligned.FallbackReason, DiscardFallbackNotAlignedToReclaim)
	}
	if unaligned.LogicalZeroBytes != 4096 {
		t.Fatalf("unaligned fallback should count logical zero bytes: %+v", unaligned)
	}
}

func TestDiscardObservationECGeometry(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:                HexVolumeID(101),
		SizeBytes:         1 << 30,
		RedundancyBackend: RedundancyBackendEC,
		ECDataShards:      4,
		ECStripeUnitBytes: 128 << 10,
		ExtentPageBytes:   4 << 20,
	})

	obs := NewDiscardZeroFallbackObservation(spec, 512<<10, 512<<10)
	if obs.BackendType != RedundancyBackendEC {
		t.Fatalf("backend_type=%q want ec", obs.BackendType)
	}
	if obs.ReclaimGeometryBytes != 512<<10 {
		t.Fatalf("reclaim_geometry_bytes=%d want %d", obs.ReclaimGeometryBytes, 512<<10)
	}
	if !obs.AlignedToReclaimGeometry {
		t.Fatalf("expected EC stripe-aligned observation: %+v", obs)
	}
}

func TestZeroObservationKeepsZeroOperationIdentity(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		SizeBytes: 1 << 20,
	})

	obs := NewZeroObservation(spec, 0, 4096)
	if obs.Operation != IOOperationZero {
		t.Fatalf("operation=%q want zero", obs.Operation)
	}
	if obs.Policy != DiscardPolicyZero {
		t.Fatalf("policy=%q want zero", obs.Policy)
	}
	if obs.DiscardBytes != 0 || obs.LogicalZeroBytes != 4096 {
		t.Fatalf("unexpected zero byte counters: %+v", obs)
	}
}

func TestDiscardObservationTrueReclaimCountsLogicalZero(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:             HexVolumeID(101),
		SizeBytes:      1 << 20,
		ChunkSizeBytes: 64 << 10,
	})

	obs := NewDiscardTrueReclaimObservation(spec, 0, 64<<10)
	if obs.Policy != DiscardPolicyTrueReclaim {
		t.Fatalf("policy=%q want true_reclaim", obs.Policy)
	}
	if !obs.AlignedToReclaimGeometry {
		t.Fatalf("expected reclaim-aligned observation: %+v", obs)
	}
	if obs.FallbackReason != "" {
		t.Fatalf("fallback_reason=%q want empty", obs.FallbackReason)
	}
	if obs.DiscardBytes != 64<<10 || obs.LogicalZeroBytes != 64<<10 {
		t.Fatalf("unexpected byte counters: %+v", obs)
	}
}

func TestMetricsCollectorRecordsDiscardIdentity(t *testing.T) {
	collector := NewMetricsCollector()
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:             HexVolumeID(101),
		SizeBytes:      1 << 20,
		ChunkSizeBytes: 64 << 10,
	})

	collector.RecordDiscardObservation(NewZeroObservation(spec, 0, 4096))
	collector.RecordDiscardObservation(NewDiscardAlignmentZeroFallbackObservation(spec, 0, 4096))

	snapshot := collector.Snapshot()
	if snapshot.IOIdentity == nil {
		t.Fatalf("io_identity missing")
	}
	identity := snapshot.IOIdentity
	if identity.DiscardBytes != 4096 || identity.LogicalZeroBytes != 8192 {
		t.Fatalf("unexpected identity bytes: %+v", identity)
	}
	if identity.DiscardZeroFallbackBytes != 4096 || identity.DiscardTrueReclaimBytes != 0 {
		t.Fatalf("unexpected discard policy bytes: %+v", identity)
	}
	if identity.DiscardAlignedCount != 0 || identity.DiscardUnalignedCount != 1 || identity.DiscardAlignmentFallbacks != 1 {
		t.Fatalf("unexpected alignment counters: %+v", identity)
	}
	if identity.ByDiscardPolicy[DiscardPolicyZeroFallback] != 1 {
		t.Fatalf("unexpected policy map: %+v", identity.ByDiscardPolicy)
	}
	if identity.LastObservation == nil || identity.LastObservation.Operation != IOOperationDiscard {
		t.Fatalf("unexpected last observation: %+v", identity.LastObservation)
	}
}
