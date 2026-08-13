package local

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DefaultStoreID     = "default"
	DefaultStoreShards = 1
	DefaultStoreWeight = 100

	StoreStateHealthy  = "healthy"
	StoreStateDegraded = "degraded"
	StoreStateReadOnly = "read_only"
	StoreStateFailed   = "failed"
	StoreStateDraining = "draining"
)

type StoreSpec struct {
	ID     string `yaml:"id"`
	Path   string `yaml:"path"`
	Shards int    `yaml:"shards"`
	Weight int    `yaml:"weight"`
}

type StoreConfigFile struct {
	Stores []StoreSpec `yaml:"stores"`
}

type StoreWeightUpdate struct {
	StoreID string `json:"store_id"`
	Weight  int    `json:"weight"`
}

type StoreTuningUpdate struct {
	StoreID string `json:"store_id"`
	Weight  int    `json:"weight"`
}

type StoreSnapshot struct {
	ID                        string `json:"id"`
	Path                      string `json:"path"`
	Shards                    int    `json:"shards"`
	Weight                    int    `json:"weight"`
	AllocationWeight          int    `json:"allocation_weight"`
	State                     string `json:"state"`
	CapacityBytes             uint64 `json:"capacity_bytes"`
	AvailableBytes            uint64 `json:"available_bytes"`
	UsedBytes                 uint64 `json:"used_bytes"`
	PebbleDiskUsageBytes      uint64 `json:"pebble_disk_usage_bytes"`
	CompactionPendingBytes    uint64 `json:"compaction_pending_bytes"`
	CompactionInProgressBytes uint64 `json:"compaction_in_progress_bytes"`
}

func ValidateStoreState(state string) error {
	switch strings.TrimSpace(state) {
	case StoreStateHealthy, StoreStateDegraded, StoreStateReadOnly, StoreStateFailed, StoreStateDraining:
		return nil
	default:
		return fmt.Errorf("invalid store state %q", state)
	}
}

func ParseStoreSpec(raw string) (StoreSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return StoreSpec{}, fmt.Errorf("store spec is empty")
	}
	spec := StoreSpec{
		Shards: DefaultStoreShards,
		Weight: DefaultStoreWeight,
	}
	pathPart := raw
	if idx := strings.Index(raw, ":"); idx >= 0 {
		pathPart = raw[:idx]
		options := raw[idx+1:]
		for _, opt := range strings.Split(options, ",") {
			opt = strings.TrimSpace(opt)
			if opt == "" {
				continue
			}
			key, value, ok := strings.Cut(opt, "=")
			if !ok {
				return StoreSpec{}, fmt.Errorf("invalid store option %q", opt)
			}
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			switch key {
			case "id", "store_id":
				spec.ID = value
			case "shards":
				parsed, err := strconv.Atoi(value)
				if err != nil {
					return StoreSpec{}, fmt.Errorf("invalid shards %q", value)
				}
				spec.Shards = parsed
			case "weight":
				parsed, err := strconv.Atoi(value)
				if err != nil {
					return StoreSpec{}, fmt.Errorf("invalid weight %q", value)
				}
				spec.Weight = parsed
			default:
				return StoreSpec{}, fmt.Errorf("unknown store option %q", key)
			}
		}
	}
	spec.Path = strings.TrimSpace(pathPart)
	return spec, nil
}

func LoadStoreConfigFile(path string) ([]StoreSpec, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("store config path is required")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg StoreConfigFile
	dec := yaml.NewDecoder(strings.NewReader(string(payload)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode store config %s: %w", path, err)
	}
	if len(cfg.Stores) == 0 {
		return nil, fmt.Errorf("store config %s must define at least one store", path)
	}
	return normalizeStoreSpecs("", cfg.Stores)
}

func UpdateStoreWeightsInConfigFile(path string, updates []StoreWeightUpdate) ([]StoreSpec, error) {
	stores, err := LoadStoreConfigFile(path)
	if err != nil {
		return nil, err
	}
	updated, err := ApplyStoreWeightUpdates(stores, updates)
	if err != nil {
		return nil, err
	}
	if err := WriteStoreConfigFile(path, updated); err != nil {
		return nil, err
	}
	return updated, nil
}

func UpdateStoreTuningInConfigFile(path string, updates []StoreTuningUpdate) ([]StoreSpec, error) {
	stores, err := LoadStoreConfigFile(path)
	if err != nil {
		return nil, err
	}
	updated, err := ApplyStoreTuningUpdates(stores, updates)
	if err != nil {
		return nil, err
	}
	if err := WriteStoreConfigFile(path, updated); err != nil {
		return nil, err
	}
	return updated, nil
}

func WriteStoreConfigFile(path string, stores []StoreSpec) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("store config path is required")
	}
	normalized, err := normalizeStoreSpecs("", stores)
	if err != nil {
		return err
	}
	payload, err := yaml.Marshal(StoreConfigFile{Stores: normalized})
	if err != nil {
		return fmt.Errorf("marshal store config %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp store config: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	if _, err := tmp.Write(payload); err != nil {
		cleanup()
		return fmt.Errorf("write temp store config: %w", err)
	}
	if err := tmp.Chmod(0o644); err != nil {
		cleanup()
		return fmt.Errorf("chmod temp store config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close temp store config: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("replace store config %s: %w", path, err)
	}
	return nil
}

func ApplyStoreWeightUpdates(stores []StoreSpec, updates []StoreWeightUpdate) ([]StoreSpec, error) {
	normalized, err := normalizeStoreSpecs("", stores)
	if err != nil {
		return nil, err
	}
	if len(updates) == 0 {
		return nil, fmt.Errorf("store weight updates are required")
	}
	updatesByID := make(map[string]int, len(updates))
	for _, update := range updates {
		storeID := strings.TrimSpace(update.StoreID)
		if storeID == "" {
			return nil, fmt.Errorf("store_id is required")
		}
		if update.Weight < 0 {
			return nil, fmt.Errorf("store %q weight must be zero or greater", storeID)
		}
		if _, exists := updatesByID[storeID]; exists {
			return nil, fmt.Errorf("duplicate store weight update for %q", storeID)
		}
		updatesByID[storeID] = update.Weight
	}
	if len(updatesByID) != len(normalized) {
		return nil, fmt.Errorf("store weight updates must cover the current store set: current=%d requested=%d", len(normalized), len(updatesByID))
	}
	reloaded := make([]StoreSpec, 0, len(normalized))
	for _, current := range normalized {
		weight, ok := updatesByID[current.ID]
		if !ok {
			return nil, fmt.Errorf("missing store weight update for %q", current.ID)
		}
		reloaded = append(reloaded, StoreSpec{
			ID:     current.ID,
			Path:   current.Path,
			Shards: current.Shards,
			Weight: weight,
		})
		delete(updatesByID, current.ID)
	}
	for storeID := range updatesByID {
		return nil, fmt.Errorf("unknown store weight update for %q", storeID)
	}
	return reloaded, nil
}

func ApplyStoreTuningUpdates(stores []StoreSpec, updates []StoreTuningUpdate) ([]StoreSpec, error) {
	normalized, err := normalizeStoreSpecs("", stores)
	if err != nil {
		return nil, err
	}
	if len(updates) == 0 {
		return nil, fmt.Errorf("store tuning updates are required")
	}
	updatesByID := make(map[string]StoreTuningUpdate, len(updates))
	for _, update := range updates {
		storeID := strings.TrimSpace(update.StoreID)
		if storeID == "" {
			return nil, fmt.Errorf("store_id is required")
		}
		if update.Weight < 0 {
			return nil, fmt.Errorf("store %q weight must be zero or greater", storeID)
		}
		if _, exists := updatesByID[storeID]; exists {
			return nil, fmt.Errorf("duplicate store tuning update for %q", storeID)
		}
		update.StoreID = storeID
		updatesByID[storeID] = update
	}
	if len(updatesByID) != len(normalized) {
		return nil, fmt.Errorf("store tuning updates must cover the current store set: current=%d requested=%d", len(normalized), len(updatesByID))
	}
	reloaded := make([]StoreSpec, 0, len(normalized))
	for _, current := range normalized {
		update, ok := updatesByID[current.ID]
		if !ok {
			return nil, fmt.Errorf("missing store tuning update for %q", current.ID)
		}
		reloaded = append(reloaded, StoreSpec{
			ID:     current.ID,
			Path:   current.Path,
			Shards: current.Shards,
			Weight: update.Weight,
		})
		delete(updatesByID, current.ID)
	}
	for storeID := range updatesByID {
		return nil, fmt.Errorf("unknown store tuning update for %q", storeID)
	}
	return reloaded, nil
}

func normalizeStoreSpecs(path string, stores []StoreSpec) ([]StoreSpec, error) {
	if len(stores) == 0 {
		if strings.TrimSpace(path) == "" {
			return nil, fmt.Errorf("path is required")
		}
		return []StoreSpec{{
			ID:     DefaultStoreID,
			Path:   path,
			Shards: DefaultStoreShards,
			Weight: DefaultStoreWeight,
		}}, nil
	}

	seenIDs := make(map[string]struct{}, len(stores))
	seenPaths := make(map[string]struct{}, len(stores))
	normalized := make([]StoreSpec, 0, len(stores))
	for i, spec := range stores {
		spec.Path = strings.TrimSpace(spec.Path)
		spec.ID = strings.TrimSpace(spec.ID)
		if spec.ID == "" {
			spec.ID = fmt.Sprintf("store-%d", i)
		}
		if spec.Path == "" {
			return nil, fmt.Errorf("store %q path is required", spec.ID)
		}
		if spec.Shards <= 0 {
			return nil, fmt.Errorf("store %q shards must be greater than zero", spec.ID)
		}
		if spec.Weight < 0 {
			return nil, fmt.Errorf("store %q weight must be zero or greater", spec.ID)
		}
		if _, ok := seenIDs[spec.ID]; ok {
			return nil, fmt.Errorf("duplicate store id %q", spec.ID)
		}
		cleanPath := filepath.Clean(spec.Path)
		if _, ok := seenPaths[cleanPath]; ok {
			return nil, fmt.Errorf("duplicate store path %q", spec.Path)
		}
		seenIDs[spec.ID] = struct{}{}
		seenPaths[cleanPath] = struct{}{}
		normalized = append(normalized, spec)
	}
	return normalized, nil
}
