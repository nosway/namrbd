package main

import (
	"context"
	"fmt"
	"log"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

const maintenanceThrottleAuthority = "sbs-service-maintenance-throttle"

type maintenanceThrottleRecord struct {
	Authority               string `json:"authority"`
	Generation              uint64 `json:"generation"`
	MaxConcurrentRepairs    int    `json:"max_concurrent_repairs"`
	MaxConcurrentRebalances int    `json:"max_concurrent_rebalances"`
	MaxConcurrentDrains     int    `json:"max_concurrent_drains"`
	MaxConcurrentPayloadGCs int    `json:"max_concurrent_payload_gcs"`
	PauseRepairs            bool   `json:"pause_repairs"`
	PauseRebalances         bool   `json:"pause_rebalances"`
	PauseDrains             bool   `json:"pause_drains"`
	PausePayloadGCs         bool   `json:"pause_payload_gcs"`
	CreatedBy               string `json:"created_by,omitempty"`
	CreatedReason           string `json:"created_reason,omitempty"`
	CreatedAtUnix           int64  `json:"created_at_unix,omitempty"`
	UpdatedAtUnix           int64  `json:"updated_at_unix,omitempty"`
}

func (s *server) loadMaintenanceSettingsSnapshot(ctx context.Context) (maintenanceSnapshot, error) {
	defaults := maintenanceThrottleRecordFromSnapshot(s.maint.snapshot())
	rec, err := getBackupJSON[maintenanceThrottleRecord](ctx, s.kv, maintenanceThrottleKey(s.root))
	if err != nil {
		if errorsIsNotFound(err) {
			return defaults.toSnapshot(), nil
		}
		return defaults.toSnapshot(), err
	}
	rec = rec.withDefaults(defaults)
	s.maint.applySnapshot(rec.toSnapshot())
	return rec.toSnapshot(), nil
}

func (s *server) effectiveMaintenanceSettingsSnapshot(ctx context.Context) maintenanceSnapshot {
	settings, err := s.loadMaintenanceSettingsSnapshot(ctx)
	if err != nil {
		log.Printf("sbs-service maintenance throttle load failed; using local settings: %v", err)
		return s.maint.snapshot()
	}
	return settings
}

func (s *server) updateMaintenanceThrottleRecord(ctx context.Context, meta *adminv1.RequestMeta, mutate func(*maintenanceThrottleRecord) bool) (maintenanceSnapshot, error) {
	now := time.Now().UTC()
	defaults := maintenanceThrottleRecordFromSnapshot(s.maint.snapshot())
	rec := defaults
	err := clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		existing, err := getBackupJSON[maintenanceThrottleRecord](ctx, tx, maintenanceThrottleKey(s.root))
		if err != nil && !errorsIsNotFound(err) {
			return err
		}
		if err == nil {
			rec = existing.withDefaults(defaults)
		} else {
			rec = defaults
		}
		changed := mutate(&rec)
		if !changed {
			return nil
		}
		rec.Generation++
		if rec.Generation == 0 {
			rec.Generation = 1
		}
		if rec.CreatedAtUnix == 0 {
			rec.CreatedAtUnix = now.Unix()
			if meta != nil {
				rec.CreatedBy = meta.GetActor()
				rec.CreatedReason = meta.GetReason()
			}
		}
		rec.UpdatedAtUnix = now.Unix()
		return putBackupJSON(ctx, tx, maintenanceThrottleKey(s.root), rec)
	})
	if err != nil {
		return defaults.toSnapshot(), err
	}
	snapshot := rec.toSnapshot()
	s.maint.applySnapshot(snapshot)
	return snapshot, nil
}

func maintenanceThrottleRecordFromSnapshot(settings maintenanceSnapshot) maintenanceThrottleRecord {
	if settings.generation == 0 {
		settings.generation = 1
	}
	return maintenanceThrottleRecord{
		Authority:               maintenanceThrottleAuthority,
		Generation:              settings.generation,
		MaxConcurrentRepairs:    maxInt(settings.maxConcurrentRepairs, 1),
		MaxConcurrentRebalances: maxInt(settings.maxConcurrentRebalances, 1),
		MaxConcurrentDrains:     maxInt(settings.maxConcurrentDrains, 1),
		MaxConcurrentPayloadGCs: maxInt(settings.maxConcurrentPayloadGCs, 1),
		PauseRepairs:            settings.pauseRepairs,
		PauseRebalances:         settings.pauseRebalances,
		PauseDrains:             settings.pauseDrains,
		PausePayloadGCs:         settings.pausePayloadGCs,
	}
}

func (r maintenanceThrottleRecord) withDefaults(defaults maintenanceThrottleRecord) maintenanceThrottleRecord {
	if r.Authority == "" {
		r.Authority = maintenanceThrottleAuthority
	}
	if r.Generation == 0 {
		r.Generation = maxUint64(defaults.Generation, 1)
	}
	if r.MaxConcurrentRepairs <= 0 {
		r.MaxConcurrentRepairs = maxInt(defaults.MaxConcurrentRepairs, 1)
	}
	if r.MaxConcurrentRebalances <= 0 {
		r.MaxConcurrentRebalances = maxInt(defaults.MaxConcurrentRebalances, 1)
	}
	if r.MaxConcurrentDrains <= 0 {
		r.MaxConcurrentDrains = maxInt(defaults.MaxConcurrentDrains, 1)
	}
	if r.MaxConcurrentPayloadGCs <= 0 {
		r.MaxConcurrentPayloadGCs = maxInt(defaults.MaxConcurrentPayloadGCs, 1)
	}
	return r
}

func (r maintenanceThrottleRecord) toSnapshot() maintenanceSnapshot {
	return maintenanceSnapshot{
		generation:              maxUint64(r.Generation, 1),
		maxConcurrentRepairs:    maxInt(r.MaxConcurrentRepairs, 1),
		maxConcurrentRebalances: maxInt(r.MaxConcurrentRebalances, 1),
		maxConcurrentDrains:     maxInt(r.MaxConcurrentDrains, 1),
		maxConcurrentPayloadGCs: maxInt(r.MaxConcurrentPayloadGCs, 1),
		pauseRepairs:            r.PauseRepairs,
		pauseRebalances:         r.PauseRebalances,
		pauseDrains:             r.PauseDrains,
		pausePayloadGCs:         r.PausePayloadGCs,
	}
}

func (m *maintenanceSettings) applySnapshot(settings maintenanceSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.generation = maxUint64(settings.generation, 1)
	m.maxConcurrentRepairs = maxInt(settings.maxConcurrentRepairs, 1)
	m.maxConcurrentRebalances = maxInt(settings.maxConcurrentRebalances, 1)
	m.maxConcurrentDrains = maxInt(settings.maxConcurrentDrains, 1)
	m.maxConcurrentPayloadGCs = maxInt(settings.maxConcurrentPayloadGCs, 1)
	m.pauseRepairs = settings.pauseRepairs
	m.pauseRebalances = settings.pauseRebalances
	m.pauseDrains = settings.pauseDrains
	m.pausePayloadGCs = settings.pausePayloadGCs
}

func maintenanceThrottleKey(root string) string {
	return fmt.Sprintf("%s/admin/maintenance-throttle", root)
}
