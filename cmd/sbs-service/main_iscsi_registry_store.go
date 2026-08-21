package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nosway/namrbd/iscsi"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

const (
	iscsiServingRegistryAuthority = "sbs_service_tikv"
	iscsiRegistryLayoutLegacy     = "monolith_v1"
	iscsiRegistryLayoutSplit      = "split_v2"
	iscsiRegistryManifestSuffix   = "admin/iscsi/registry/v2/manifest"
	iscsiRegistryObjectRootSuffix = "admin/iscsi/registry/v2/objects"
	iscsiRegistryChangeRootSuffix = "admin/iscsi/registry/v2/changes"

	iscsiRegistryDefaultPageSize = 128
	iscsiRegistryMaxPageSize     = 128
	iscsiRegistryChangeRetention = 4096
)

const (
	iscsiObjectPortal      = "portals"
	iscsiObjectTarget      = "targets"
	iscsiObjectLUN         = "luns"
	iscsiObjectACL         = "acls"
	iscsiObjectSession     = "sessions"
	iscsiObjectFailover    = "failovers"
	iscsiObjectExport      = "exports"
	iscsiObjectIdempotency = "idempotency"
)

var iscsiRegistryObjectKinds = []string{
	iscsiObjectPortal,
	iscsiObjectTarget,
	iscsiObjectLUN,
	iscsiObjectACL,
	iscsiObjectSession,
	iscsiObjectFailover,
	iscsiObjectExport,
	iscsiObjectIdempotency,
}

var (
	errISCSIRegistryRevisionMismatch = errors.New("iscsi registry revision mismatch")
	errISCSIRegistryRevisionChanged  = errors.New("iscsi registry revision changed")
	errISCSIRegistryPageToken        = errors.New("invalid iscsi registry page token")
)

type iscsiRegistryManifest struct {
	Version               int                         `json:"version"`
	StorageLayout         string                      `json:"storage_layout"`
	ServingAuthority      string                      `json:"serving_registry_authority"`
	RegistryRevision      uint64                      `json:"registry_revision"`
	ConfigGeneration      uint64                      `json:"config_generation"`
	UpdatedAtUnix         int64                       `json:"updated_at_unix,omitempty"`
	PortalCount           uint64                      `json:"portal_count"`
	TargetCount           uint64                      `json:"target_count"`
	LUNCount              uint64                      `json:"lun_count"`
	ExportCount           uint64                      `json:"export_count"`
	InitiatorACLCount     uint64                      `json:"initiator_acl_count"`
	SessionCount          uint64                      `json:"session_count"`
	FailoverCount         uint64                      `json:"failover_count"`
	ChangeFloorRevision   uint64                      `json:"change_floor_revision"`
	ObservabilityCounters iscsi.ObservabilityCounters `json:"observability_counters"`
}

type iscsiRegistryExportChangeRecord struct {
	RegistryRevision uint64                     `json:"registry_revision"`
	ConfigGeneration uint64                     `json:"config_generation"`
	Operation        string                     `json:"operation"`
	ExportID         string                     `json:"export_id"`
	Export           *iscsiExportRegistryRecord `json:"export,omitempty"`
}

type iscsiRegistryChangePage struct {
	Changes            []iscsiRegistryExportChangeRecord
	NextPageToken      string
	FromRevision       uint64
	ToRevision         uint64
	CheckpointRevision uint64
	Manifest           iscsiRegistryManifest
	ResyncRequired     bool
	ResyncReason       string
}

type iscsiExportRegistryRecord struct {
	ExportID                   string   `json:"export_id"`
	TargetIQN                  string   `json:"target_iqn"`
	LUNID                      uint64   `json:"lun_id"`
	LUNWWN                     string   `json:"lun_wwn"`
	VolumeID                   string   `json:"volume_id"`
	ExportMode                 string   `json:"export_mode"`
	LogicalBlockSizeBytes      uint64   `json:"logical_block_size_bytes"`
	Enabled                    bool     `json:"enabled"`
	PortalIDs                  []string `json:"portal_ids,omitempty"`
	ActiveISCSIGatewayID       string   `json:"active_iscsi_gateway_id,omitempty"`
	StandbyISCSIGatewayIDs     []string `json:"standby_iscsi_gateway_ids,omitempty"`
	ExportLeaseID              string   `json:"export_lease_id,omitempty"`
	ExportEpoch                uint64   `json:"export_epoch"`
	FailoverState              string   `json:"failover_state,omitempty"`
	WriterPolicy               string   `json:"writer_policy,omitempty"`
	HAFailoverMode             string   `json:"ha_failover_mode,omitempty"`
	ReadWriteAllowed           bool     `json:"read_write_allowed"`
	LastWriteRejectionReason   string   `json:"last_write_rejection_reason,omitempty"`
	LastRejectedISCSIGatewayID string   `json:"last_rejected_iscsi_gateway_id,omitempty"`
}

type iscsiRegistryBatchReader interface {
	BatchGet(ctx context.Context, keys []string) (map[string][]byte, error)
}

func (s *server) iscsiRegistryKey() string {
	return s.iscsiRegistryRootedKey(iscsiRegistryStateSuffix)
}

func (s *server) iscsiRegistryManifestKey() string {
	return s.iscsiRegistryRootedKey(iscsiRegistryManifestSuffix)
}

func (s *server) iscsiRegistryRootedKey(suffix string) string {
	root := strings.Trim(strings.TrimSpace(s.root), "/")
	if root == "" {
		return suffix
	}
	return root + "/" + strings.TrimLeft(suffix, "/")
}

func (s *server) iscsiRegistryObjectPrefix(kind string) string {
	return s.iscsiRegistryRootedKey(iscsiRegistryObjectRootSuffix+"/"+kind) + "/"
}

func (s *server) iscsiRegistryObjectKey(kind, id string) string {
	return s.iscsiRegistryObjectPrefix(kind) + base64.RawURLEncoding.EncodeToString([]byte(id))
}

func (s *server) iscsiRegistryChangePrefix() string {
	return s.iscsiRegistryRootedKey(iscsiRegistryChangeRootSuffix) + "/"
}

func (s *server) iscsiRegistryChangeRevisionPrefix(revision uint64) string {
	return fmt.Sprintf("%s%020d/", s.iscsiRegistryChangePrefix(), revision)
}

func (s *server) iscsiRegistryChangeKey(revision uint64, exportID string) string {
	return s.iscsiRegistryChangeRevisionPrefix(revision) + base64.RawURLEncoding.EncodeToString([]byte(exportID))
}

func decodeISCSIRegistryObjectID(key string) (string, error) {
	encoded := key[strings.LastIndex(key, "/")+1:]
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("decode iscsi registry object key %q: %w", key, err)
	}
	return string(raw), nil
}

func (s *server) loadISCSIRegistryManifest(ctx context.Context) (iscsiRegistryManifest, bool, error) {
	raw, found, err := s.kv.Get(ctx, s.iscsiRegistryManifestKey())
	if err != nil || !found {
		return iscsiRegistryManifest{}, found, err
	}
	var manifest iscsiRegistryManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return iscsiRegistryManifest{}, false, err
	}
	if manifest.StorageLayout != iscsiRegistryLayoutSplit {
		return iscsiRegistryManifest{}, false, fmt.Errorf("unsupported iscsi registry storage layout %q", manifest.StorageLayout)
	}
	return manifest, true, nil
}

func (s *server) loadISCSIRegistrySummary(ctx context.Context) (iscsiRegistryManifest, error) {
	manifest, found, err := s.loadISCSIRegistryManifest(ctx)
	if err != nil || found {
		return manifest, err
	}
	state, err := s.loadLegacyISCSIRegistryState(ctx)
	if err != nil {
		return iscsiRegistryManifest{}, err
	}
	_, manifest, err = buildISCSIRegistryObjects(state)
	if err != nil {
		return iscsiRegistryManifest{}, err
	}
	manifest.StorageLayout = state.storageLayout
	return manifest, nil
}

func (m iscsiRegistryManifest) empty() bool {
	return m.PortalCount+m.TargetCount+m.LUNCount+m.ExportCount+
		m.InitiatorACLCount+m.SessionCount+m.FailoverCount == 0
}

func (s *server) listISCSIExportRegistryPage(ctx context.Context, pageSize int, pageToken string, revision uint64) ([]iscsiExportRegistryRecord, string, iscsiRegistryManifest, error) {
	manifest, found, err := s.loadISCSIRegistryManifest(ctx)
	if err != nil {
		return nil, "", iscsiRegistryManifest{}, err
	}
	if !found {
		return s.listLegacyISCSIExportRegistryPage(ctx, pageSize, pageToken, revision)
	}
	if revision != 0 && revision != manifest.RegistryRevision {
		return nil, "", manifest, fmt.Errorf("%w: current %d requested %d", errISCSIRegistryRevisionMismatch, manifest.RegistryRevision, revision)
	}
	prefix := s.iscsiRegistryObjectPrefix(iscsiObjectExport)
	if pageToken != "" && !strings.HasPrefix(pageToken, prefix) {
		return nil, "", manifest, fmt.Errorf("%w: token is outside the export registry prefix", errISCSIRegistryPageToken)
	}
	keys, next, err := s.kv.List(ctx, prefix, pageToken, pageSize)
	if err != nil {
		return nil, "", manifest, err
	}
	values, err := s.readISCSIRegistryObjectValues(ctx, keys)
	if err != nil {
		return nil, "", manifest, err
	}
	records := make([]iscsiExportRegistryRecord, 0, len(keys))
	for _, key := range keys {
		raw, ok := values[key]
		if !ok {
			return nil, "", manifest, fmt.Errorf("iscsi export object %q disappeared during page read", key)
		}
		var record iscsiExportRegistryRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, "", manifest, fmt.Errorf("decode iscsi export object %q: %w", key, err)
		}
		records = append(records, record)
	}
	after, stillFound, err := s.loadISCSIRegistryManifest(ctx)
	if err != nil {
		return nil, "", manifest, err
	}
	if !stillFound || after.RegistryRevision != manifest.RegistryRevision || after.ConfigGeneration != manifest.ConfigGeneration {
		return nil, "", manifest, fmt.Errorf("%w during export page read", errISCSIRegistryRevisionChanged)
	}
	return records, next, manifest, nil
}

func (s *server) listLegacyISCSIExportRegistryPage(ctx context.Context, pageSize int, pageToken string, revision uint64) ([]iscsiExportRegistryRecord, string, iscsiRegistryManifest, error) {
	state, err := s.loadLegacyISCSIRegistryState(ctx)
	if err != nil {
		return nil, "", iscsiRegistryManifest{}, err
	}
	_, manifest, err := buildISCSIRegistryObjects(state)
	if err != nil {
		return nil, "", iscsiRegistryManifest{}, err
	}
	manifest.StorageLayout = state.storageLayout
	if revision != 0 && revision != manifest.RegistryRevision {
		return nil, "", manifest, fmt.Errorf("%w: current %d requested %d", errISCSIRegistryRevisionMismatch, manifest.RegistryRevision, revision)
	}
	exports := buildISCSIExportRegistryRecords(state)
	ids := make([]string, 0, len(exports))
	for id := range exports {
		if pageToken == "" || id > pageToken {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > pageSize {
		ids = ids[:pageSize]
	}
	records := make([]iscsiExportRegistryRecord, 0, len(ids))
	for _, id := range ids {
		records = append(records, exports[id])
	}
	next := ""
	if len(ids) == pageSize {
		next = ids[len(ids)-1]
	}
	return records, next, manifest, nil
}

func (s *server) getISCSIExportRegistryRecord(ctx context.Context, exportID string) (iscsiExportRegistryRecord, iscsiRegistryManifest, bool, error) {
	manifest, found, err := s.loadISCSIRegistryManifest(ctx)
	if err != nil {
		return iscsiExportRegistryRecord{}, iscsiRegistryManifest{}, false, err
	}
	if !found {
		state, err := s.loadLegacyISCSIRegistryState(ctx)
		if err != nil {
			return iscsiExportRegistryRecord{}, iscsiRegistryManifest{}, false, err
		}
		_, manifest, err = buildISCSIRegistryObjects(state)
		if err != nil {
			return iscsiExportRegistryRecord{}, iscsiRegistryManifest{}, false, err
		}
		manifest.StorageLayout = state.storageLayout
		record, ok := buildISCSIExportRegistryRecords(state)[exportID]
		return record, manifest, ok, nil
	}
	raw, objectFound, err := s.kv.Get(ctx, s.iscsiRegistryObjectKey(iscsiObjectExport, exportID))
	if err != nil || !objectFound {
		return iscsiExportRegistryRecord{}, manifest, objectFound, err
	}
	var record iscsiExportRegistryRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return iscsiExportRegistryRecord{}, manifest, false, err
	}
	after, stillFound, err := s.loadISCSIRegistryManifest(ctx)
	if err != nil {
		return iscsiExportRegistryRecord{}, manifest, false, err
	}
	if !stillFound || after.RegistryRevision != manifest.RegistryRevision || after.ConfigGeneration != manifest.ConfigGeneration {
		return iscsiExportRegistryRecord{}, manifest, false, fmt.Errorf("%w during export point read", errISCSIRegistryRevisionChanged)
	}
	return record, manifest, true, nil
}

func (s *server) listISCSIRegistryChanges(ctx context.Context, afterRevision uint64, pageSize int, pageToken string) (iscsiRegistryChangePage, error) {
	manifest, err := s.loadISCSIRegistrySummary(ctx)
	if err != nil {
		return iscsiRegistryChangePage{}, err
	}
	page := iscsiRegistryChangePage{FromRevision: afterRevision, ToRevision: afterRevision, Manifest: manifest}
	if afterRevision > manifest.RegistryRevision {
		return page, fmt.Errorf("%w: checkpoint %d is ahead of current %d", errISCSIRegistryRevisionMismatch, afterRevision, manifest.RegistryRevision)
	}
	if manifest.StorageLayout != iscsiRegistryLayoutSplit || afterRevision < manifest.ChangeFloorRevision {
		page.ResyncRequired = true
		page.ResyncReason = fmt.Sprintf("checkpoint %d is older than retained change floor %d", afterRevision, manifest.ChangeFloorRevision)
		return page, nil
	}
	if afterRevision == manifest.RegistryRevision {
		page.ToRevision = manifest.RegistryRevision
		page.CheckpointRevision = manifest.RegistryRevision
		return page, nil
	}
	prefix := s.iscsiRegistryChangePrefix()
	cursor := strings.TrimSpace(pageToken)
	if cursor != "" && !strings.HasPrefix(cursor, prefix) {
		return page, fmt.Errorf("%w: token is outside the registry change prefix", errISCSIRegistryPageToken)
	}
	if cursor == "" {
		cursor = s.iscsiRegistryChangeRevisionPrefix(afterRevision) + "\xff"
	}
	keys, next, err := s.kv.List(ctx, prefix, cursor, pageSize)
	if err != nil {
		return page, err
	}
	values, err := s.readISCSIRegistryObjectValues(ctx, keys)
	if err != nil {
		return page, err
	}
	page.Changes = make([]iscsiRegistryExportChangeRecord, 0, len(keys))
	for _, key := range keys {
		raw, ok := values[key]
		if !ok {
			return page, fmt.Errorf("iscsi registry change %q disappeared during page read", key)
		}
		var change iscsiRegistryExportChangeRecord
		if err := json.Unmarshal(raw, &change); err != nil {
			return page, fmt.Errorf("decode iscsi registry change %q: %w", key, err)
		}
		if change.RegistryRevision <= afterRevision || change.RegistryRevision > manifest.RegistryRevision {
			continue
		}
		page.Changes = append(page.Changes, change)
		if change.RegistryRevision > page.ToRevision {
			page.ToRevision = change.RegistryRevision
		}
	}
	after, found, err := s.loadISCSIRegistryManifest(ctx)
	if err != nil {
		return page, err
	}
	if !found || after.RegistryRevision != manifest.RegistryRevision || after.ConfigGeneration != manifest.ConfigGeneration {
		return page, fmt.Errorf("%w during changed-set page read", errISCSIRegistryRevisionChanged)
	}
	page.NextPageToken = next
	if next == "" {
		page.ToRevision = manifest.RegistryRevision
		page.CheckpointRevision = manifest.RegistryRevision
	}
	return page, nil
}

func (s *server) loadISCSIRegistryState(ctx context.Context) (*iscsiRegistryState, error) {
	manifest, found, err := s.loadISCSIRegistryManifest(ctx)
	if err != nil {
		return nil, err
	}
	if !found {
		return s.loadLegacyISCSIRegistryState(ctx)
	}
	state := newISCSIRegistryState()
	state.RegistryRevision = manifest.RegistryRevision
	state.ConfigGeneration = manifest.ConfigGeneration
	state.UpdatedAtUnix = manifest.UpdatedAtUnix
	state.storageLayout = iscsiRegistryLayoutSplit
	state.changeFloorRevision = manifest.ChangeFloorRevision
	for _, kind := range []string{
		iscsiObjectPortal, iscsiObjectTarget, iscsiObjectLUN, iscsiObjectACL,
		iscsiObjectSession, iscsiObjectFailover, iscsiObjectIdempotency,
	} {
		if err := s.loadAllISCSIRegistryObjects(ctx, state, kind); err != nil {
			return nil, err
		}
	}
	after, ok, err := s.loadISCSIRegistryManifest(ctx)
	if err != nil {
		return nil, err
	}
	if !ok || after.RegistryRevision != manifest.RegistryRevision || after.ConfigGeneration != manifest.ConfigGeneration {
		return nil, fmt.Errorf("iscsi registry revision changed during split-state read")
	}
	return state.Normalize(), nil
}

func (s *server) loadLegacyISCSIRegistryState(ctx context.Context) (*iscsiRegistryState, error) {
	raw, found, err := s.kv.Get(ctx, s.iscsiRegistryKey())
	if err != nil {
		return nil, err
	}
	if !found {
		state := newISCSIRegistryState()
		state.storageLayout = iscsiRegistryLayoutSplit
		return state, nil
	}
	var state iscsiRegistryState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, err
	}
	state.storageLayout = iscsiRegistryLayoutLegacy
	state.changeFloorRevision = state.RegistryRevision
	return state.Normalize(), nil
}

func (s *server) loadAllISCSIRegistryObjects(ctx context.Context, state *iscsiRegistryState, kind string) error {
	cursor := ""
	for {
		keys, next, err := s.kv.List(ctx, s.iscsiRegistryObjectPrefix(kind), cursor, iscsiRegistryDefaultPageSize)
		if err != nil {
			return err
		}
		values, err := s.readISCSIRegistryObjectValues(ctx, keys)
		if err != nil {
			return err
		}
		for _, key := range keys {
			raw, ok := values[key]
			if !ok {
				return fmt.Errorf("iscsi registry object %q disappeared during read", key)
			}
			if err := decodeISCSIRegistryStateObject(state, kind, key, raw); err != nil {
				return err
			}
		}
		if next == "" {
			return nil
		}
		cursor = next
	}
}

func (s *server) readISCSIRegistryObjectValues(ctx context.Context, keys []string) (map[string][]byte, error) {
	if batch, ok := s.kv.(iscsiRegistryBatchReader); ok {
		return batch.BatchGet(ctx, keys)
	}
	values := make(map[string][]byte, len(keys))
	for _, key := range keys {
		raw, found, err := s.kv.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if found {
			values[key] = raw
		}
	}
	return values, nil
}

func decodeISCSIRegistryStateObject(state *iscsiRegistryState, kind, key string, raw []byte) error {
	id, err := decodeISCSIRegistryObjectID(key)
	if err != nil {
		return err
	}
	switch kind {
	case iscsiObjectPortal:
		var value iscsi.Portal
		err = json.Unmarshal(raw, &value)
		state.Portals[id] = value
	case iscsiObjectTarget:
		var value iscsi.Target
		err = json.Unmarshal(raw, &value)
		state.Targets[id] = value
	case iscsiObjectLUN:
		var value iscsi.LUN
		err = json.Unmarshal(raw, &value)
		state.LUNs[id] = value
	case iscsiObjectACL:
		var value iscsi.InitiatorACL
		err = json.Unmarshal(raw, &value)
		state.ACLs[id] = value
	case iscsiObjectSession:
		var value iscsi.Session
		err = json.Unmarshal(raw, &value)
		state.Sessions[id] = value
	case iscsiObjectFailover:
		var value iscsi.FailoverRuntime
		err = json.Unmarshal(raw, &value)
		state.Failovers[id] = value
	case iscsiObjectIdempotency:
		var value iscsiRegistryIdempotency
		err = json.Unmarshal(raw, &value)
		state.IdempotencyRecords[id] = value
	default:
		return fmt.Errorf("unsupported iscsi registry object kind %q", kind)
	}
	if err != nil {
		return fmt.Errorf("decode iscsi registry %s object %q: %w", kind, id, err)
	}
	return nil
}

func (s *server) saveISCSIRegistryState(ctx context.Context, state *iscsiRegistryState) error {
	if state == nil {
		return fmt.Errorf("iscsi registry state is nil")
	}
	state.Normalize()
	if state.UpdatedAtUnix == 0 {
		state.UpdatedAtUnix = time.Now().UTC().Unix()
	}
	state.changeFloorRevision = state.RegistryRevision
	objects, manifest, err := buildISCSIRegistryObjects(state)
	if err != nil {
		return err
	}
	existing, err := s.listAllISCSIRegistryObjectKeys(ctx)
	if err != nil {
		return err
	}
	existingChanges, err := s.listAllISCSIRegistryChangeKeys(ctx)
	if err != nil {
		return err
	}
	existing = append(existing, existingChanges...)
	if err := clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		for _, key := range existing {
			if err := tx.Delete(ctx, key); err != nil {
				return err
			}
		}
		if err := writeISCSIRegistryObjects(ctx, tx, s, objects); err != nil {
			return err
		}
		if err := writeISCSIRegistryManifest(ctx, tx, s.iscsiRegistryManifestKey(), manifest); err != nil {
			return err
		}
		// The manifest is the v2 commit marker. Once it exists, remove the v1
		// authority key so a rollback cannot silently serve stale mappings.
		return tx.Delete(ctx, s.iscsiRegistryKey())
	}); err != nil {
		return err
	}
	state.storageLayout = iscsiRegistryLayoutSplit
	return nil
}

func (s *server) saveISCSIRegistryStateDelta(ctx context.Context, before, after *iscsiRegistryState) error {
	if before == nil || after == nil || before.storageLayout == iscsiRegistryLayoutLegacy {
		return s.saveISCSIRegistryState(ctx, after)
	}
	beforeObjects, _, err := buildISCSIRegistryObjects(before)
	if err != nil {
		return err
	}
	after.changeFloorRevision = before.changeFloorRevision
	if after.RegistryRevision > iscsiRegistryChangeRetention {
		retainedFloor := after.RegistryRevision - iscsiRegistryChangeRetention
		if retainedFloor > after.changeFloorRevision {
			after.changeFloorRevision = retainedFloor
		}
	}
	afterObjects, manifest, err := buildISCSIRegistryObjects(after)
	if err != nil {
		return err
	}
	changes, err := buildISCSIRegistryExportChanges(beforeObjects[iscsiObjectExport], afterObjects[iscsiObjectExport], after.RegistryRevision, after.ConfigGeneration)
	if err != nil {
		return err
	}
	retiredChangeKeys, err := s.listRetiredISCSIRegistryChangeKeys(ctx, before.changeFloorRevision, after.changeFloorRevision)
	if err != nil {
		return err
	}
	if err := clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		for _, key := range retiredChangeKeys {
			if err := tx.Delete(ctx, key); err != nil {
				return err
			}
		}
		for kind, oldValues := range beforeObjects {
			for id := range oldValues {
				if _, ok := afterObjects[kind][id]; !ok {
					if err := tx.Delete(ctx, s.iscsiRegistryObjectKey(kind, id)); err != nil {
						return err
					}
				}
			}
		}
		for kind, values := range afterObjects {
			for id, raw := range values {
				if old, ok := beforeObjects[kind][id]; ok && bytes.Equal(old, raw) {
					continue
				}
				if err := tx.Set(ctx, s.iscsiRegistryObjectKey(kind, id), raw); err != nil {
					return err
				}
			}
		}
		for _, change := range changes {
			raw, err := json.Marshal(change)
			if err != nil {
				return err
			}
			if err := tx.Set(ctx, s.iscsiRegistryChangeKey(change.RegistryRevision, change.ExportID), raw); err != nil {
				return err
			}
		}
		return writeISCSIRegistryManifest(ctx, tx, s.iscsiRegistryManifestKey(), manifest)
	}); err != nil {
		return err
	}
	after.storageLayout = iscsiRegistryLayoutSplit
	return nil
}

func buildISCSIRegistryExportChanges(before, after map[string][]byte, revision, generation uint64) ([]iscsiRegistryExportChangeRecord, error) {
	ids := make([]string, 0, len(before)+len(after))
	seen := map[string]bool{}
	for id := range before {
		seen[id] = true
		ids = append(ids, id)
	}
	for id := range after {
		if !seen[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	changes := make([]iscsiRegistryExportChangeRecord, 0)
	for _, id := range ids {
		oldRaw, oldFound := before[id]
		newRaw, newFound := after[id]
		if oldFound && newFound && bytes.Equal(oldRaw, newRaw) {
			continue
		}
		change := iscsiRegistryExportChangeRecord{
			RegistryRevision: revision, ConfigGeneration: generation, ExportID: id,
		}
		if !newFound {
			change.Operation = "delete"
			changes = append(changes, change)
			continue
		}
		var record iscsiExportRegistryRecord
		if err := json.Unmarshal(newRaw, &record); err != nil {
			return nil, fmt.Errorf("decode changed iscsi export %q: %w", id, err)
		}
		change.Operation = "upsert"
		change.Export = &record
		changes = append(changes, change)
	}
	return changes, nil
}

func (s *server) listRetiredISCSIRegistryChangeKeys(ctx context.Context, oldFloor, newFloor uint64) ([]string, error) {
	if newFloor <= oldFloor {
		return nil, nil
	}
	var keys []string
	for revision := oldFloor + 1; revision <= newFloor; revision++ {
		prefix := s.iscsiRegistryChangeRevisionPrefix(revision)
		cursor := ""
		for {
			page, next, err := s.kv.List(ctx, prefix, cursor, iscsiRegistryMaxPageSize)
			if err != nil {
				return nil, err
			}
			keys = append(keys, page...)
			if next == "" {
				break
			}
			cursor = next
		}
	}
	return keys, nil
}

func buildISCSIRegistryObjects(state *iscsiRegistryState) (map[string]map[string][]byte, iscsiRegistryManifest, error) {
	state.Normalize()
	objects := make(map[string]map[string][]byte, len(iscsiRegistryObjectKinds))
	for _, kind := range iscsiRegistryObjectKinds {
		objects[kind] = map[string][]byte{}
	}
	marshalMap := func(kind string, values map[string]any) error {
		for id, value := range values {
			raw, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("encode iscsi registry %s object %q: %w", kind, id, err)
			}
			objects[kind][id] = raw
		}
		return nil
	}
	if err := marshalMap(iscsiObjectPortal, portalAnyMap(state.Portals)); err != nil {
		return nil, iscsiRegistryManifest{}, err
	}
	if err := marshalMap(iscsiObjectTarget, targetAnyMap(state.Targets)); err != nil {
		return nil, iscsiRegistryManifest{}, err
	}
	if err := marshalMap(iscsiObjectLUN, lunAnyMap(state.LUNs)); err != nil {
		return nil, iscsiRegistryManifest{}, err
	}
	if err := marshalMap(iscsiObjectACL, aclAnyMap(state.ACLs)); err != nil {
		return nil, iscsiRegistryManifest{}, err
	}
	if err := marshalMap(iscsiObjectSession, sessionAnyMap(state.Sessions)); err != nil {
		return nil, iscsiRegistryManifest{}, err
	}
	if err := marshalMap(iscsiObjectFailover, failoverAnyMap(state.Failovers)); err != nil {
		return nil, iscsiRegistryManifest{}, err
	}
	if err := marshalMap(iscsiObjectIdempotency, idempotencyAnyMap(state.IdempotencyRecords)); err != nil {
		return nil, iscsiRegistryManifest{}, err
	}
	exports := buildISCSIExportRegistryRecords(state)
	if err := marshalMap(iscsiObjectExport, exportAnyMap(exports)); err != nil {
		return nil, iscsiRegistryManifest{}, err
	}
	manifest := iscsiRegistryManifest{
		Version: iscsiRegistryStateVersion, StorageLayout: iscsiRegistryLayoutSplit,
		ServingAuthority: iscsiServingRegistryAuthority, RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration, UpdatedAtUnix: state.UpdatedAtUnix,
		PortalCount: uint64(len(state.Portals)), TargetCount: uint64(len(state.Targets)),
		LUNCount: uint64(len(state.LUNs)), ExportCount: uint64(len(exports)),
		InitiatorACLCount: uint64(len(state.ACLs)), SessionCount: uint64(len(state.Sessions)),
		FailoverCount:         uint64(len(state.Failovers)),
		ChangeFloorRevision:   state.changeFloorRevision,
		ObservabilityCounters: iscsi.BuildObservabilityCounters(state.controlState()),
	}
	return objects, manifest, nil
}

func buildISCSIExportRegistryRecords(state *iscsiRegistryState) map[string]iscsiExportRegistryRecord {
	out := make(map[string]iscsiExportRegistryRecord, len(state.LUNs))
	for _, lun := range state.LUNs {
		exportID := strings.TrimSpace(lun.ExportID)
		if exportID == "" {
			continue
		}
		target := state.Targets[lun.TargetIQN]
		portalIDs := append([]string(nil), target.PortalIDs...)
		if len(portalIDs) == 0 && target.PortalID != "" {
			portalIDs = []string{target.PortalID}
		}
		sort.Strings(portalIDs)
		runtime := iscsi.NormalizeFailoverRuntime(state.Failovers[exportID])
		readWriteAllowed := lun.Enabled && lun.ExportMode == "read_write"
		if runtime.ExportID != "" {
			readWriteAllowed = readWriteAllowed && runtime.ActiveISCSIGatewayID != "" && runtime.State == "active"
		}
		out[exportID] = iscsiExportRegistryRecord{
			ExportID: exportID, TargetIQN: lun.TargetIQN, LUNID: lun.LUNID, LUNWWN: lun.LUNWWN,
			VolumeID: lun.VolumeID, ExportMode: lun.ExportMode, LogicalBlockSizeBytes: lun.LogicalBlockSizeBytes,
			Enabled: lun.Enabled, PortalIDs: portalIDs, ActiveISCSIGatewayID: runtime.ActiveISCSIGatewayID,
			StandbyISCSIGatewayIDs: append([]string(nil), runtime.StandbyISCSIGatewayIDs...),
			ExportLeaseID:          runtime.ExportLeaseID, ExportEpoch: runtime.ExportEpoch, FailoverState: runtime.State,
			WriterPolicy: runtime.WriterPolicy, HAFailoverMode: runtime.HAFailoverMode,
			ReadWriteAllowed: readWriteAllowed, LastWriteRejectionReason: runtime.LastWriteRejectionReason,
			LastRejectedISCSIGatewayID: runtime.LastRejectedISCSIGatewayID,
		}
	}
	return out
}

func writeISCSIRegistryObjects(ctx context.Context, tx clustermeta.ReadWriter, s *server, objects map[string]map[string][]byte) error {
	for kind, values := range objects {
		for id, raw := range values {
			if err := tx.Set(ctx, s.iscsiRegistryObjectKey(kind, id), raw); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeISCSIRegistryManifest(ctx context.Context, tx clustermeta.ReadWriter, key string, manifest iscsiRegistryManifest) error {
	raw, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return tx.Set(ctx, key, raw)
}

func (s *server) listAllISCSIRegistryObjectKeys(ctx context.Context) ([]string, error) {
	var out []string
	for _, kind := range iscsiRegistryObjectKinds {
		cursor := ""
		for {
			keys, next, err := s.kv.List(ctx, s.iscsiRegistryObjectPrefix(kind), cursor, iscsiRegistryMaxPageSize)
			if err != nil {
				return nil, err
			}
			out = append(out, keys...)
			if next == "" {
				break
			}
			cursor = next
		}
	}
	return out, nil
}

func (s *server) listAllISCSIRegistryChangeKeys(ctx context.Context) ([]string, error) {
	prefix := s.iscsiRegistryChangePrefix()
	cursor := ""
	var out []string
	for {
		keys, next, err := s.kv.List(ctx, prefix, cursor, iscsiRegistryMaxPageSize)
		if err != nil {
			return nil, err
		}
		out = append(out, keys...)
		if next == "" {
			return out, nil
		}
		cursor = next
	}
}

func cloneISCSIRegistryState(state *iscsiRegistryState) (*iscsiRegistryState, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	var cloned iscsiRegistryState
	if err := json.Unmarshal(raw, &cloned); err != nil {
		return nil, err
	}
	cloned.storageLayout = state.storageLayout
	cloned.changeFloorRevision = state.changeFloorRevision
	return cloned.Normalize(), nil
}

func portalAnyMap(in map[string]iscsi.Portal) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func targetAnyMap(in map[string]iscsi.Target) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func lunAnyMap(in map[string]iscsi.LUN) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func aclAnyMap(in map[string]iscsi.InitiatorACL) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func sessionAnyMap(in map[string]iscsi.Session) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func failoverAnyMap(in map[string]iscsi.FailoverRuntime) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func idempotencyAnyMap(in map[string]iscsiRegistryIdempotency) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func exportAnyMap(in map[string]iscsiExportRegistryRecord) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
