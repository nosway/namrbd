package metadata

import (
	"context"
	"strings"
	"testing"
)

func TestValidateECProfileDefaultsAndAlignment(t *testing.T) {
	rec := NormalizeECProfile(ECProfileRecord{
		ProfileID:    "ec-6-3",
		DataShards:   6,
		ParityShards: 3,
	})
	if rec.CodecID != ECCodecRSVandGF8 {
		t.Fatalf("codec_id=%q want %q", rec.CodecID, ECCodecRSVandGF8)
	}
	if rec.StripeUnitBytes != DefaultECStripeUnitBytes {
		t.Fatalf("stripe_unit_bytes=%d want %d", rec.StripeUnitBytes, DefaultECStripeUnitBytes)
	}
	if rec.FailureDomain != ECFailureDomainZone {
		t.Fatalf("failure_domain=%q want %q", rec.FailureDomain, ECFailureDomainZone)
	}
	if rec.MaxUnavailableFailureDomains != 1 || rec.MaxShardsPerFailureDomain != 3 {
		t.Fatalf("failure-domain caps=%d/%d", rec.MaxUnavailableFailureDomains, rec.MaxShardsPerFailureDomain)
	}
	if err := ValidateECProfile(rec, ECProfileValidationOptions{
		BlockSizeBytes:           4096,
		AllocationChunkSizeBytes: 64 << 10,
	}); err != nil {
		t.Fatalf("ValidateECProfile: %v", err)
	}
}

func TestValidateECProfileRejectsInvalidShapes(t *testing.T) {
	tests := []struct {
		name string
		rec  ECProfileRecord
		want string
	}{
		{
			name: "unsupported codec",
			rec:  ECProfileRecord{ProfileID: "ec-6-3", CodecID: "xor", DataShards: 6, ParityShards: 3},
			want: "unsupported ec codec_id",
		},
		{
			name: "product cap",
			rec:  ECProfileRecord{ProfileID: "ec-wide", DataShards: 30, ParityShards: 3},
			want: "product cap",
		},
		{
			name: "stripe alignment",
			rec:  ECProfileRecord{ProfileID: "ec-6-3", DataShards: 6, ParityShards: 3, StripeUnitBytes: 96 << 10},
			want: "allocation chunk size",
		},
		{
			name: "profile id",
			rec:  ECProfileRecord{ProfileID: "bad/profile", DataShards: 6, ParityShards: 3},
			want: "profile_id must not contain",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateECProfile(tc.rec, ECProfileValidationOptions{AllocationChunkSizeBytes: 64 << 10})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateECProfile error=%v want contains %q", err, tc.want)
			}
		})
	}
}

func TestValidateECProfileAllowsExplicitLabOverride(t *testing.T) {
	rec := ECProfileRecord{
		ProfileID:    "ec-lab-wide",
		DataShards:   30,
		ParityShards: 3,
		LabOverride:  true,
	}
	if err := ValidateECProfile(rec, ECProfileValidationOptions{}); err != nil {
		t.Fatalf("ValidateECProfile with lab override: %v", err)
	}
}

func TestRepositoryECProfileRoundTripAndListFilter(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-k-test")

	active := ECProfileRecord{ProfileID: "ec-6-3", DataShards: 6, ParityShards: 3, CreatedAtUnix: 10}
	disabled := ECProfileRecord{ProfileID: "ec-8-4", DataShards: 8, ParityShards: 4, Lifecycle: ECProfileLifecycleDisabled, CreatedAtUnix: 11}
	if err := repo.PutECProfile(ctx, active); err != nil {
		t.Fatalf("PutECProfile active: %v", err)
	}
	if err := repo.PutECProfile(ctx, disabled); err != nil {
		t.Fatalf("PutECProfile disabled: %v", err)
	}

	got, err := repo.GetECProfile(ctx, "ec-6-3")
	if err != nil {
		t.Fatalf("GetECProfile: %v", err)
	}
	if got.CodecID != ECCodecRSVandGF8 || got.StripeUnitBytes != DefaultECStripeUnitBytes {
		t.Fatalf("profile defaults not normalized: %+v", got)
	}

	activeOnly, err := repo.ListECProfiles(ctx, false)
	if err != nil {
		t.Fatalf("ListECProfiles active: %v", err)
	}
	if len(activeOnly) != 1 || activeOnly[0].ProfileID != "ec-6-3" {
		t.Fatalf("active profiles=%+v", activeOnly)
	}
	all, err := repo.ListECProfiles(ctx, true)
	if err != nil {
		t.Fatalf("ListECProfiles all: %v", err)
	}
	if len(all) != 2 || all[0].ProfileID != "ec-6-3" || all[1].ProfileID != "ec-8-4" {
		t.Fatalf("all profiles=%+v", all)
	}
}
