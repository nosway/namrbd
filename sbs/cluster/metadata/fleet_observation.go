package metadata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const FleetObservationSchemaVersion = 1

type FleetObservation struct {
	SchemaVersion               int                    `json:"schema_version"`
	SourceRevision              uint64                 `json:"source_revision"`
	ManifestRevision            string                 `json:"manifest_revision"`
	ManifestDigest              string                 `json:"manifest_digest"`
	BinaryDigest                string                 `json:"binary_digest"`
	ConfigDigest                string                 `json:"config_digest"`
	StoreDigest                 string                 `json:"store_digest"`
	ApplyOperationID            string                 `json:"apply_operation_id,omitempty"`
	ApplyState                  string                 `json:"apply_state"`
	ApplyPaused                 bool                   `json:"apply_paused"`
	HostCheckFailedCount        uint64                 `json:"host_check_failed_count"`
	ConfigDriftCount            uint64                 `json:"config_drift_count"`
	StrayNodeCount              uint64                 `json:"stray_node_count"`
	StorageClaimMismatchCount   uint64                 `json:"storage_claim_mismatch_count"`
	Zones                       []FleetZoneObservation `json:"zones,omitempty"`
	StoreCount                  uint64                 `json:"store_count"`
	UsableBytes                 uint64                 `json:"usable_bytes"`
	FreeBytes                   uint64                 `json:"free_bytes"`
	ReservedBytes               uint64                 `json:"reserved_bytes"`
	MissingNodeCount            uint64                 `json:"missing_node_count"`
	StaleNodeCount              uint64                 `json:"stale_node_count"`
	CapacityObservedAtUnix      int64                  `json:"capacity_observed_at_unix"`
	RepairOldestAgeSeconds      uint64                 `json:"repair_oldest_age_seconds"`
	RebalanceOldestAgeSeconds   uint64                 `json:"rebalance_oldest_age_seconds"`
	DrainOldestAgeSeconds       uint64                 `json:"drain_oldest_age_seconds"`
	RepairClaimLatencyMillis    uint64                 `json:"repair_claim_latency_millis"`
	RebalanceClaimLatencyMillis uint64                 `json:"rebalance_claim_latency_millis"`
	DrainClaimLatencyMillis     uint64                 `json:"drain_claim_latency_millis"`
	ObservedAtUnix              int64                  `json:"observed_at_unix"`
	RecordDigest                string                 `json:"record_digest"`
}

type FleetZoneObservation struct {
	Zone          string `json:"zone"`
	ActiveNodes   uint64 `json:"active_nodes"`
	DrainingNodes uint64 `json:"draining_nodes"`
	SuspectNodes  uint64 `json:"suspect_nodes"`
	DownNodes     uint64 `json:"down_nodes"`
}

func NewFleetObservation(observation FleetObservation) (FleetObservation, error) {
	observation.SchemaVersion = FleetObservationSchemaVersion
	observation.ManifestRevision = strings.TrimSpace(observation.ManifestRevision)
	observation.ApplyOperationID = strings.TrimSpace(observation.ApplyOperationID)
	observation.ApplyState = strings.ToLower(strings.TrimSpace(observation.ApplyState))
	observation.RecordDigest = digestFleetObservation(observation)
	if err := ValidateFleetObservation(observation); err != nil {
		return FleetObservation{}, err
	}
	return observation, nil
}

func ValidateFleetObservation(observation FleetObservation) error {
	if observation.SchemaVersion != FleetObservationSchemaVersion || observation.SourceRevision == 0 || observation.ManifestRevision == "" || observation.CapacityObservedAtUnix <= 0 || observation.ObservedAtUnix <= 0 {
		return fmt.Errorf("invalid fleet observation identity or freshness")
	}
	for name, value := range map[string]string{
		"manifest_digest": observation.ManifestDigest,
		"binary_digest":   observation.BinaryDigest,
		"config_digest":   observation.ConfigDigest,
		"store_digest":    observation.StoreDigest,
	} {
		if !validFleetDigest(value) {
			return fmt.Errorf("%s must be a canonical sha256 digest", name)
		}
	}
	switch observation.ApplyState {
	case "idle", "running", "paused", "completed", "failed":
	default:
		return fmt.Errorf("invalid fleet apply state %q", observation.ApplyState)
	}
	if observation.ApplyPaused != (observation.ApplyState == "paused") {
		return fmt.Errorf("apply_paused must match apply_state")
	}
	if observation.ApplyState != "idle" && observation.ApplyOperationID == "" {
		return fmt.Errorf("non-idle apply state requires apply_operation_id")
	}
	if observation.RecordDigest != digestFleetObservation(observation) {
		return fmt.Errorf("fleet observation digest mismatch")
	}
	if len(observation.Zones) > 64 {
		return fmt.Errorf("fleet observation has more than 64 zones")
	}
	previousZone := ""
	for _, zone := range observation.Zones {
		if !validSummaryIdentifier(zone.Zone) || zone.Zone <= previousZone {
			return fmt.Errorf("fleet observation zones must be unique and sorted")
		}
		previousZone = zone.Zone
	}
	return nil
}

func (r *Repository) PutFleetObservation(ctx context.Context, observation FleetObservation) error {
	if r == nil {
		return fmt.Errorf("nil fleet observation repository")
	}
	if err := ValidateFleetObservation(observation); err != nil {
		return err
	}
	return r.putJSON(ctx, fleetObservationKey(r.root), observation)
}

func (r *Repository) GetFleetObservation(ctx context.Context) (FleetObservation, error) {
	if r == nil {
		return FleetObservation{}, fmt.Errorf("nil fleet observation repository")
	}
	var observation FleetObservation
	if err := r.getJSON(ctx, fleetObservationKey(r.root), &observation); err != nil {
		return FleetObservation{}, err
	}
	if err := ValidateFleetObservation(observation); err != nil {
		return FleetObservation{}, err
	}
	return observation, nil
}

func digestFleetObservation(observation FleetObservation) string {
	observation.RecordDigest = ""
	raw, _ := json.Marshal(observation)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validFleetDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func fleetObservationKey(root string) string {
	return fmt.Sprintf("%s/derived/ad/v1/fleet-observation", root)
}
