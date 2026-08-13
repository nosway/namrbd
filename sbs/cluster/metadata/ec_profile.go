package metadata

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

const (
	ECCodecRSVandGF8         = "rs_vand_gf8"
	ECFailureDomainZone      = "zone"
	DefaultECStripeUnitBytes = uint32(128 << 10)
	ECProductShardCap        = uint32(32)
	ECGF8ShardLimit          = uint32(256)
)

type ECProfileLifecycle string

const (
	ECProfileLifecycleActive   ECProfileLifecycle = "active"
	ECProfileLifecycleDisabled ECProfileLifecycle = "disabled"
)

type ECProfileRecord struct {
	ProfileID                    string             `json:"profile_id"`
	CodecID                      string             `json:"codec_id"`
	DataShards                   uint32             `json:"data_shards"`
	ParityShards                 uint32             `json:"parity_shards"`
	StripeUnitBytes              uint32             `json:"stripe_unit_bytes"`
	FailureDomain                string             `json:"failure_domain"`
	MaxUnavailableFailureDomains uint32             `json:"max_unavailable_failure_domains"`
	MaxShardsPerFailureDomain    uint32             `json:"max_shards_per_failure_domain"`
	ProductShardCap              uint32             `json:"product_shard_cap,omitempty"`
	Lifecycle                    ECProfileLifecycle `json:"lifecycle"`
	LabOverride                  bool               `json:"lab_override,omitempty"`
	CreatedBy                    string             `json:"created_by,omitempty"`
	CreatedReason                string             `json:"created_reason,omitempty"`
	CreatedAtUnix                int64              `json:"created_at_unix"`
	UpdatedAtUnix                int64              `json:"updated_at_unix,omitempty"`
}

type ECProfileValidationOptions struct {
	BlockSizeBytes           uint32
	AllocationChunkSizeBytes uint32
	AllowLabOverride         bool
}

func NormalizeECProfile(rec ECProfileRecord) ECProfileRecord {
	rec.ProfileID = strings.TrimSpace(rec.ProfileID)
	rec.CodecID = strings.TrimSpace(rec.CodecID)
	if rec.CodecID == "" {
		rec.CodecID = ECCodecRSVandGF8
	}
	if rec.StripeUnitBytes == 0 {
		rec.StripeUnitBytes = DefaultECStripeUnitBytes
	}
	rec.FailureDomain = strings.TrimSpace(rec.FailureDomain)
	if rec.FailureDomain == "" {
		rec.FailureDomain = ECFailureDomainZone
	}
	if rec.MaxUnavailableFailureDomains == 0 {
		rec.MaxUnavailableFailureDomains = 1
	}
	if rec.MaxShardsPerFailureDomain == 0 {
		rec.MaxShardsPerFailureDomain = rec.ParityShards
	}
	if rec.ProductShardCap == 0 {
		rec.ProductShardCap = ECProductShardCap
	}
	if rec.Lifecycle == "" {
		rec.Lifecycle = ECProfileLifecycleActive
	}
	return rec
}

func ValidateECProfile(rec ECProfileRecord, opts ECProfileValidationOptions) error {
	rec = NormalizeECProfile(rec)
	if err := ValidateECProfileID(rec.ProfileID); err != nil {
		return err
	}
	if rec.CodecID != ECCodecRSVandGF8 {
		return fmt.Errorf("unsupported ec codec_id %q", rec.CodecID)
	}
	if rec.DataShards == 0 {
		return fmt.Errorf("data_shards is required")
	}
	if rec.ParityShards == 0 {
		return fmt.Errorf("parity_shards is required")
	}
	total := rec.DataShards + rec.ParityShards
	if total > ECGF8ShardLimit {
		return fmt.Errorf("ec profile shard count exceeds GF(2^8) limit: have=%d max=%d", total, ECGF8ShardLimit)
	}
	allowLabOverride := opts.AllowLabOverride || rec.LabOverride
	if total > ECProductShardCap && !allowLabOverride {
		return fmt.Errorf("ec profile shard count exceeds product cap: have=%d max=%d", total, ECProductShardCap)
	}
	if rec.StripeUnitBytes == 0 {
		return fmt.Errorf("stripe_unit_bytes is required")
	}
	if rec.StripeUnitBytes%4096 != 0 {
		return fmt.Errorf("stripe_unit_bytes must be 4KiB aligned")
	}
	if opts.BlockSizeBytes > 0 && rec.StripeUnitBytes%opts.BlockSizeBytes != 0 {
		return fmt.Errorf("stripe_unit_bytes must be aligned to block size")
	}
	if opts.AllocationChunkSizeBytes > 0 && rec.StripeUnitBytes%opts.AllocationChunkSizeBytes != 0 {
		return fmt.Errorf("stripe_unit_bytes must be aligned to allocation chunk size")
	}
	if rec.FailureDomain != ECFailureDomainZone {
		return fmt.Errorf("unsupported ec failure_domain %q", rec.FailureDomain)
	}
	if rec.MaxUnavailableFailureDomains != 1 {
		return fmt.Errorf("max_unavailable_failure_domains must be 1")
	}
	if rec.MaxShardsPerFailureDomain != rec.ParityShards {
		return fmt.Errorf("max_shards_per_failure_domain must equal parity_shards")
	}
	switch rec.Lifecycle {
	case ECProfileLifecycleActive, ECProfileLifecycleDisabled:
	default:
		return fmt.Errorf("unsupported ec profile lifecycle %q", rec.Lifecycle)
	}
	return nil
}

func ValidateECProfileID(profileID string) error {
	profileID = strings.TrimSpace(profileID)
	if profileID == "" {
		return fmt.Errorf("profile_id is required")
	}
	if strings.Contains(profileID, "/") {
		return fmt.Errorf("profile_id must not contain '/'")
	}
	return nil
}

func (r *Repository) PutECProfile(ctx context.Context, rec ECProfileRecord) error {
	rec = NormalizeECProfile(rec)
	if err := ValidateECProfile(rec, ECProfileValidationOptions{}); err != nil {
		return err
	}
	return r.putJSON(ctx, ecProfileKey(r.root, rec.ProfileID), rec)
}

func (r *Repository) GetECProfile(ctx context.Context, profileID string) (ECProfileRecord, error) {
	if err := ValidateECProfileID(profileID); err != nil {
		return ECProfileRecord{}, err
	}
	var rec ECProfileRecord
	if err := r.getJSON(ctx, ecProfileKey(r.root, strings.TrimSpace(profileID)), &rec); err != nil {
		return ECProfileRecord{}, err
	}
	return NormalizeECProfile(rec), nil
}

func (r *Repository) ListECProfiles(ctx context.Context, includeDisabled bool) ([]ECProfileRecord, error) {
	keys, err := r.listAll(ctx, ecProfilesPrefix(r.root))
	if err != nil {
		return nil, err
	}
	out := make([]ECProfileRecord, 0, len(keys))
	for _, key := range keys {
		var rec ECProfileRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		rec = NormalizeECProfile(rec)
		if !includeDisabled && rec.Lifecycle == ECProfileLifecycleDisabled {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ProfileID < out[j].ProfileID })
	return out, nil
}

func ecProfilesPrefix(root string) string {
	return fmt.Sprintf("%s/ec/profiles/", root)
}

func ecProfileKey(root, profileID string) string {
	return fmt.Sprintf("%s%s", ecProfilesPrefix(root), strings.TrimSpace(profileID))
}
