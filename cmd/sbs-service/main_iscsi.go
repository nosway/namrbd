package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/iscsi"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	iscsiRegistryStateVersion = 1
	iscsiRegistryStateSuffix  = "admin/iscsi/registry/state"
)

type iscsiRegistryState struct {
	Version             int                                 `json:"version"`
	RegistryRevision    uint64                              `json:"registry_revision"`
	ConfigGeneration    uint64                              `json:"config_generation"`
	UpdatedAtUnix       int64                               `json:"updated_at_unix,omitempty"`
	Portals             map[string]iscsi.Portal             `json:"portals"`
	Targets             map[string]iscsi.Target             `json:"targets"`
	LUNs                map[string]iscsi.LUN                `json:"luns"`
	ACLs                map[string]iscsi.InitiatorACL       `json:"initiator_acls"`
	Sessions            map[string]iscsi.Session            `json:"sessions"`
	Failovers           map[string]iscsi.FailoverRuntime    `json:"failovers"`
	IdempotencyRecords  map[string]iscsiRegistryIdempotency `json:"idempotency_records,omitempty"`
	storageLayout       string                              `json:"-"`
	changeFloorRevision uint64                              `json:"-"`
}

type iscsiRegistryIdempotency struct {
	Kind             string `json:"kind"`
	ResourceKey      string `json:"resource_key"`
	OperationID      string `json:"operation_id,omitempty"`
	RegistryRevision uint64 `json:"registry_revision"`
	ConfigGeneration uint64 `json:"config_generation"`
	Message          string `json:"message,omitempty"`
}

func newISCSIRegistryState() *iscsiRegistryState {
	return (&iscsiRegistryState{Version: iscsiRegistryStateVersion}).Normalize()
}

func (r *iscsiRegistryState) Normalize() *iscsiRegistryState {
	if r.Version == 0 {
		r.Version = iscsiRegistryStateVersion
	}
	if r.Portals == nil {
		r.Portals = map[string]iscsi.Portal{}
	}
	if r.Targets == nil {
		r.Targets = map[string]iscsi.Target{}
	}
	if r.LUNs == nil {
		r.LUNs = map[string]iscsi.LUN{}
	}
	if r.ACLs == nil {
		r.ACLs = map[string]iscsi.InitiatorACL{}
	}
	if r.Sessions == nil {
		r.Sessions = map[string]iscsi.Session{}
	}
	if r.Failovers == nil {
		r.Failovers = map[string]iscsi.FailoverRuntime{}
	}
	if r.IdempotencyRecords == nil {
		r.IdempotencyRecords = map[string]iscsiRegistryIdempotency{}
	}
	for key, portal := range r.Portals {
		portal = iscsi.NormalizePortal(portal)
		r.Portals[key] = portal
		if portal.PortalID != "" && key != portal.PortalID {
			delete(r.Portals, key)
			r.Portals[portal.PortalID] = portal
		}
	}
	for key, target := range r.Targets {
		target = iscsi.NormalizeTarget(target)
		r.Targets[key] = target
		if target.TargetIQN != "" && key != target.TargetIQN {
			delete(r.Targets, key)
			r.Targets[target.TargetIQN] = target
		}
	}
	for key, lun := range r.LUNs {
		lun.TargetIQN = strings.TrimSpace(lun.TargetIQN)
		lun.ExportID = strings.TrimSpace(lun.ExportID)
		lun.VolumeID = strings.TrimSpace(lun.VolumeID)
		lun.ExportMode = strings.TrimSpace(lun.ExportMode)
		lun.LUNWWN = strings.TrimSpace(lun.LUNWWN)
		if lun.LUNWWN == "" {
			lun.LUNWWN = iscsi.LUNWWN(lun.ExportID)
		}
		r.LUNs[key] = lun
		if lun.TargetIQN != "" {
			canonicalKey := iscsi.LUNKey(lun.TargetIQN, lun.LUNID)
			if key != canonicalKey {
				delete(r.LUNs, key)
				r.LUNs[canonicalKey] = lun
			}
		}
	}
	for key, acl := range r.ACLs {
		acl.InitiatorIQN = strings.TrimSpace(acl.InitiatorIQN)
		acl.TargetIQN = strings.TrimSpace(acl.TargetIQN)
		acl.AuthMode = strings.TrimSpace(acl.AuthMode)
		acl.CHAPSecretRef = strings.TrimSpace(acl.CHAPSecretRef)
		r.ACLs[key] = acl
		if acl.InitiatorIQN != "" && acl.TargetIQN != "" {
			canonicalKey := iscsi.ACLKey(acl.InitiatorIQN, acl.TargetIQN)
			if key != canonicalKey {
				delete(r.ACLs, key)
				r.ACLs[canonicalKey] = acl
			}
		}
	}
	for key, session := range r.Sessions {
		session = iscsi.NormalizeSession(session)
		r.Sessions[key] = session
		if session.SessionID != "" && key != session.SessionID {
			delete(r.Sessions, key)
			r.Sessions[session.SessionID] = session
		}
	}
	for key, runtime := range r.Failovers {
		runtime = iscsi.NormalizeFailoverRuntime(runtime)
		r.Failovers[key] = runtime
		if runtime.ExportID != "" && key != runtime.ExportID {
			delete(r.Failovers, key)
			r.Failovers[runtime.ExportID] = runtime
		}
	}
	return r
}

func (r *iscsiRegistryState) controlState() *iscsi.ControlState {
	if r == nil {
		return iscsi.NewControlState()
	}
	r.Normalize()
	return (&iscsi.ControlState{
		Version:   iscsi.ControlStateVersion,
		Portals:   r.Portals,
		Targets:   r.Targets,
		LUNs:      r.LUNs,
		ACLs:      r.ACLs,
		Sessions:  r.Sessions,
		Failovers: r.Failovers,
	}).Normalize()
}

func (s *server) GetISCSIRegistry(ctx context.Context, req *adminv1.GetISCSIRegistryRequest) (*adminv1.GetISCSIRegistryResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	manifest, err := s.loadISCSIRegistrySummary(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry summary: %v", err)
	}
	if req.GetSummaryOnly() {
		return iscsiRegistrySummaryResponse(cluster, manifest), nil
	}
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	control := state.controlState()
	resp := &adminv1.GetISCSIRegistryResponse{
		Cluster:               cluster,
		RegistryRevision:      state.RegistryRevision,
		ConfigGeneration:      state.ConfigGeneration,
		Portals:               iscsiPortalSummaries(control),
		Targets:               iscsiTargetSummaries(control),
		Luns:                  iscsiLUNSummaries(control, ""),
		InitiatorAcls:         iscsiACLSummaries(control, ""),
		Sessions:              iscsiSessionSummaries(control, "", "", false),
		Failovers:             iscsiFailoverSummaries(control),
		ObservabilityCounters: iscsiObservabilityCountersToProto(iscsi.BuildObservabilityCounters(control)),
	}
	applyISCSIRegistryManifest(resp, manifest)
	return resp, nil
}

func iscsiRegistrySummaryResponse(cluster *adminv1.ClusterRef, manifest iscsiRegistryManifest) *adminv1.GetISCSIRegistryResponse {
	resp := &adminv1.GetISCSIRegistryResponse{
		Cluster:               cluster,
		RegistryRevision:      manifest.RegistryRevision,
		ConfigGeneration:      manifest.ConfigGeneration,
		ObservabilityCounters: iscsiObservabilityCountersToProto(manifest.ObservabilityCounters),
	}
	applyISCSIRegistryManifest(resp, manifest)
	return resp
}

func applyISCSIRegistryManifest(resp *adminv1.GetISCSIRegistryResponse, manifest iscsiRegistryManifest) {
	resp.ServingRegistryAuthority = iscsiServingRegistryAuthority
	resp.StorageLayout = manifest.StorageLayout
	resp.RegistryEmpty = manifest.empty()
	resp.PortalCount = manifest.PortalCount
	resp.TargetCount = manifest.TargetCount
	resp.LunCount = manifest.LUNCount
	resp.ExportCount = manifest.ExportCount
	resp.InitiatorAclCount = manifest.InitiatorACLCount
	resp.SessionCount = manifest.SessionCount
	resp.FailoverCount = manifest.FailoverCount
}

func (s *server) ListISCSIPortals(ctx context.Context, req *adminv1.ListISCSIPortalsRequest) (*adminv1.ListISCSIPortalsResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	return &adminv1.ListISCSIPortalsResponse{
		Cluster:          cluster,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Portals:          iscsiPortalSummaries(state.controlState()),
	}, nil
}

func (s *server) GetISCSIPortal(ctx context.Context, req *adminv1.GetISCSIPortalRequest) (*adminv1.GetISCSIPortalResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	portalID := strings.TrimSpace(req.GetPortalId())
	if portalID == "" {
		return nil, status.Error(codes.InvalidArgument, "portal_id is required")
	}
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	portal, ok := state.controlState().Portals[portalID]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "iscsi portal %q not found", portalID)
	}
	return &adminv1.GetISCSIPortalResponse{
		Cluster:          cluster,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Portal:           iscsiPortalToProto(portal),
	}, nil
}

func (s *server) ListISCSITargets(ctx context.Context, req *adminv1.ListISCSITargetsRequest) (*adminv1.ListISCSITargetsResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	return &adminv1.ListISCSITargetsResponse{
		Cluster:          cluster,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Targets:          iscsiTargetSummaries(state.controlState()),
	}, nil
}

func (s *server) GetISCSITarget(ctx context.Context, req *adminv1.GetISCSITargetRequest) (*adminv1.GetISCSITargetResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	if targetIQN == "" {
		return nil, status.Error(codes.InvalidArgument, "target_iqn is required")
	}
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	control := state.controlState()
	target, ok := control.Targets[targetIQN]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "iscsi target %q not found", targetIQN)
	}
	return &adminv1.GetISCSITargetResponse{
		Cluster:          cluster,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Target:           iscsiTargetToProto(target),
		Luns:             iscsiLUNSummaries(control, targetIQN),
		InitiatorAcls:    iscsiACLSummaries(control, targetIQN),
		Sessions:         iscsiSessionSummaries(control, targetIQN, "", false),
	}, nil
}

func (s *server) ListISCSILUNs(ctx context.Context, req *adminv1.ListISCSILUNsRequest) (*adminv1.ListISCSILUNsResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	return &adminv1.ListISCSILUNsResponse{
		Cluster:          cluster,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Luns:             iscsiLUNSummaries(state.controlState(), targetIQN),
	}, nil
}

func (s *server) GetISCSILUN(ctx context.Context, req *adminv1.GetISCSILUNRequest) (*adminv1.GetISCSILUNResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	if targetIQN == "" {
		return nil, status.Error(codes.InvalidArgument, "target_iqn is required")
	}
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	key := iscsi.LUNKey(targetIQN, req.GetLunId())
	lun, ok := state.controlState().LUNs[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "iscsi lun %q not found", key)
	}
	return &adminv1.GetISCSILUNResponse{
		Cluster:          cluster,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Lun:              iscsiLUNToProto(lun),
	}, nil
}

func (s *server) ListISCSIExports(ctx context.Context, req *adminv1.ListISCSIExportsRequest) (*adminv1.ListISCSIExportsResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = iscsiRegistryDefaultPageSize
	}
	if pageSize > iscsiRegistryMaxPageSize {
		return nil, status.Errorf(codes.InvalidArgument, "page_size %d exceeds maximum %d", pageSize, iscsiRegistryMaxPageSize)
	}
	records, next, manifest, err := s.listISCSIExportRegistryPage(ctx, pageSize, strings.TrimSpace(req.GetPageToken()), req.GetRegistryRevision())
	if err != nil {
		switch {
		case errors.Is(err, errISCSIRegistryRevisionMismatch):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		case errors.Is(err, errISCSIRegistryRevisionChanged):
			return nil, status.Error(codes.Aborted, err.Error())
		case errors.Is(err, errISCSIRegistryPageToken):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		default:
			return nil, status.Errorf(codes.Internal, "list iscsi exports: %v", err)
		}
	}
	exports := make([]*adminv1.ISCSIExportSummary, 0, len(records))
	for _, record := range records {
		exports = append(exports, iscsiExportRegistryRecordToProto(record))
	}
	return &adminv1.ListISCSIExportsResponse{
		Cluster: cluster, RegistryRevision: manifest.RegistryRevision,
		ConfigGeneration: manifest.ConfigGeneration, Exports: exports, NextPageToken: next,
		ServingRegistryAuthority: iscsiServingRegistryAuthority, StorageLayout: manifest.StorageLayout,
	}, nil
}

func (s *server) GetISCSIExport(ctx context.Context, req *adminv1.GetISCSIExportRequest) (*adminv1.GetISCSIExportResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	exportID := strings.TrimSpace(req.GetExportId())
	if exportID == "" {
		return nil, status.Error(codes.InvalidArgument, "export_id is required")
	}
	record, manifest, found, err := s.getISCSIExportRegistryRecord(ctx, exportID)
	if err != nil {
		if errors.Is(err, errISCSIRegistryRevisionChanged) {
			return nil, status.Error(codes.Aborted, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "get iscsi export: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "iscsi export %q not found", exportID)
	}
	return &adminv1.GetISCSIExportResponse{
		Cluster: cluster, RegistryRevision: manifest.RegistryRevision,
		ConfigGeneration: manifest.ConfigGeneration, Export: iscsiExportRegistryRecordToProto(record),
		ServingRegistryAuthority: iscsiServingRegistryAuthority, StorageLayout: manifest.StorageLayout,
	}, nil
}

func (s *server) GetISCSIRegistryChanges(ctx context.Context, req *adminv1.GetISCSIRegistryChangesRequest) (*adminv1.GetISCSIRegistryChangesResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = iscsiRegistryDefaultPageSize
	}
	if pageSize > iscsiRegistryMaxPageSize {
		return nil, status.Errorf(codes.InvalidArgument, "page_size %d exceeds maximum %d", pageSize, iscsiRegistryMaxPageSize)
	}
	page, err := s.listISCSIRegistryChanges(ctx, req.GetAfterRevision(), pageSize, req.GetPageToken())
	if err != nil {
		switch {
		case errors.Is(err, errISCSIRegistryRevisionMismatch):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		case errors.Is(err, errISCSIRegistryRevisionChanged):
			return nil, status.Error(codes.Aborted, err.Error())
		case errors.Is(err, errISCSIRegistryPageToken):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		default:
			return nil, status.Errorf(codes.Internal, "get iscsi registry changes: %v", err)
		}
	}
	changes := make([]*adminv1.ISCSIRegistryExportChange, 0, len(page.Changes))
	for _, change := range page.Changes {
		item := &adminv1.ISCSIRegistryExportChange{
			RegistryRevision: change.RegistryRevision, ConfigGeneration: change.ConfigGeneration,
			Operation: change.Operation, ExportId: change.ExportID,
		}
		if change.Export != nil {
			item.Export = iscsiExportRegistryRecordToProto(*change.Export)
		}
		changes = append(changes, item)
	}
	return &adminv1.GetISCSIRegistryChangesResponse{
		Cluster: cluster, FromRevision: page.FromRevision, ToRevision: page.ToRevision,
		ConfigGeneration: page.Manifest.ConfigGeneration, Changes: changes,
		NextPageToken: page.NextPageToken, CheckpointRevision: page.CheckpointRevision,
		ResyncRequired: page.ResyncRequired, ResyncReason: page.ResyncReason,
		ChangeFloorRevision:      page.Manifest.ChangeFloorRevision,
		ServingRegistryAuthority: iscsiServingRegistryAuthority, StorageLayout: page.Manifest.StorageLayout,
	}, nil
}

func iscsiExportRegistryRecordToProto(record iscsiExportRegistryRecord) *adminv1.ISCSIExportSummary {
	return &adminv1.ISCSIExportSummary{
		ExportId: record.ExportID, TargetIqn: record.TargetIQN, LunId: record.LUNID,
		LunWwn: record.LUNWWN, VolumeId: record.VolumeID, ExportMode: record.ExportMode,
		LogicalBlockSizeBytes: record.LogicalBlockSizeBytes, Enabled: record.Enabled,
		PortalIds: append([]string(nil), record.PortalIDs...), ActiveIscsiGatewayId: record.ActiveISCSIGatewayID,
		StandbyIscsiGatewayIds: append([]string(nil), record.StandbyISCSIGatewayIDs...),
		ExportLeaseId:          record.ExportLeaseID, ExportEpoch: record.ExportEpoch,
		FailoverState: record.FailoverState, WriterPolicy: record.WriterPolicy,
		HaFailoverMode: record.HAFailoverMode, ReadWriteAllowed: record.ReadWriteAllowed,
		LastWriteRejectionReason:   record.LastWriteRejectionReason,
		LastRejectedIscsiGatewayId: record.LastRejectedISCSIGatewayID,
	}
}

func (s *server) ListISCSIInitiatorACLs(ctx context.Context, req *adminv1.ListISCSIInitiatorACLsRequest) (*adminv1.ListISCSIInitiatorACLsResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	return &adminv1.ListISCSIInitiatorACLsResponse{
		Cluster:          cluster,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		InitiatorAcls:    iscsiACLSummaries(state.controlState(), targetIQN),
	}, nil
}

func (s *server) GetISCSIInitiatorACL(ctx context.Context, req *adminv1.GetISCSIInitiatorACLRequest) (*adminv1.GetISCSIInitiatorACLResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	initiatorIQN := strings.TrimSpace(req.GetInitiatorIqn())
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	if initiatorIQN == "" || targetIQN == "" {
		return nil, status.Error(codes.InvalidArgument, "initiator_iqn and target_iqn are required")
	}
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	key := iscsi.ACLKey(initiatorIQN, targetIQN)
	acl, ok := state.controlState().ACLs[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "iscsi initiator acl %q not found", key)
	}
	return &adminv1.GetISCSIInitiatorACLResponse{
		Cluster:          cluster,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		InitiatorAcl:     iscsiACLToProto(acl),
	}, nil
}

func (s *server) ListISCSISessions(ctx context.Context, req *adminv1.ListISCSISessionsRequest) (*adminv1.ListISCSISessionsResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	initiatorIQN := strings.TrimSpace(req.GetInitiatorIqn())
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	control := state.controlState()
	return &adminv1.ListISCSISessionsResponse{
		Cluster:               cluster,
		RegistryRevision:      state.RegistryRevision,
		ConfigGeneration:      state.ConfigGeneration,
		Sessions:              iscsiSessionSummaries(control, targetIQN, initiatorIQN, req.GetConnectedOnly()),
		ObservabilityCounters: iscsiObservabilityCountersToProto(iscsi.BuildObservabilityCounters(control)),
	}, nil
}

func (s *server) GetISCSISession(ctx context.Context, req *adminv1.GetISCSISessionRequest) (*adminv1.GetISCSISessionResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	session, ok := state.controlState().Sessions[sessionID]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "iscsi session %q not found", sessionID)
	}
	return &adminv1.GetISCSISessionResponse{
		Cluster:          cluster,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Session:          iscsiSessionToProto(session),
	}, nil
}

func (s *server) RecordISCSISession(ctx context.Context, req *adminv1.RecordISCSISessionRequest) (*adminv1.RecordISCSISessionResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	session, err := iscsiSessionFromProto(req.GetSession())
	if err != nil {
		return nil, err
	}
	resourceKey := "session/" + session.SessionID
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.session.record", resourceKey, "", "iscsi session recorded", func(state *iscsiRegistryState) error {
		if _, ok := state.Targets[session.TargetIQN]; !ok {
			return status.Errorf(codes.NotFound, "iscsi target %q not found", session.TargetIQN)
		}
		if _, ok := state.LUNs[iscsi.LUNKey(session.TargetIQN, session.LUNID)]; !ok {
			return status.Errorf(codes.NotFound, "iscsi lun %s not found", iscsi.LUNKey(session.TargetIQN, session.LUNID))
		}
		state.Sessions[session.SessionID] = session
		return nil
	})
	if err != nil {
		return nil, err
	}
	if current := state.Sessions[session.SessionID]; current.SessionID != "" {
		session = current
	}
	return &adminv1.RecordISCSISessionResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Session:          iscsiSessionToProto(session),
	}, nil
}

func (s *server) DisconnectISCSISession(ctx context.Context, req *adminv1.DisconnectISCSISessionRequest) (*adminv1.DisconnectISCSISessionResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	resourceKey := "session/" + sessionID
	var session iscsi.Session
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.session.disconnect", resourceKey, "", "iscsi session disconnect requested", func(state *iscsiRegistryState) error {
		var ok bool
		session, ok = state.Sessions[sessionID]
		if !ok {
			return status.Errorf(codes.NotFound, "iscsi session %q not found", sessionID)
		}
		session = iscsi.NormalizeSession(session)
		session.Connected = false
		session.ReadWriteAllowed = false
		session.State = "disconnect_requested"
		session.LastErrorClass = "admin_disconnect"
		session.LastError = "disconnect requested by " + strings.TrimSpace(req.GetMeta().GetActor())
		state.Sessions[sessionID] = session
		return nil
	})
	if err != nil {
		return nil, err
	}
	if current := state.Sessions[sessionID]; current.SessionID != "" {
		session = current
	}
	return &adminv1.DisconnectISCSISessionResponse{
		Cluster:             cluster,
		Operation:           op,
		RegistryRevision:    state.RegistryRevision,
		ConfigGeneration:    state.ConfigGeneration,
		Session:             iscsiSessionToProto(session),
		DisconnectRequested: true,
	}, nil
}

func (s *server) GetISCSIFailover(ctx context.Context, req *adminv1.GetISCSIFailoverRequest) (*adminv1.GetISCSIFailoverResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	exportID := strings.TrimSpace(req.GetExportId())
	if exportID == "" {
		return nil, status.Error(codes.InvalidArgument, "export_id is required")
	}
	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	failover, ok := state.controlState().Failovers[exportID]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "iscsi failover runtime %q not found", exportID)
	}
	return &adminv1.GetISCSIFailoverResponse{
		Cluster:          cluster,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Failover:         iscsiFailoverToProto(failover),
	}, nil
}

func (s *server) PromoteISCSIFailover(ctx context.Context, req *adminv1.PromoteISCSIFailoverRequest) (*adminv1.PromoteISCSIFailoverResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	exportID, gatewayID, err := validateISCSIFailoverIDs(req.GetExportId(), req.GetGatewayId())
	if err != nil {
		return nil, err
	}
	resourceKey := "failover/" + exportID
	var runtime iscsi.FailoverRuntime
	volumeID := ""
	op, state, err := s.mutateISCSIRegistryWithPrecommit(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.failover.promote", resourceKey, "", "iscsi failover gateway promoted", func(state *iscsiRegistryState) error {
		if !iscsiRegistryExportExists(state, exportID) {
			return status.Errorf(codes.NotFound, "iscsi export %q not found", exportID)
		}
		runtime = iscsi.NormalizeFailoverRuntime(state.Failovers[exportID])
		if runtime.ExportID == "" {
			runtime.ExportID = exportID
		}
		if runtime.StaleGatewayRevokedID == gatewayID {
			return status.Errorf(codes.FailedPrecondition, "iscsi gateway %q is revoked stale for export %q", gatewayID, exportID)
		}
		runtime.PreviousActiveGatewayID = runtime.ActiveISCSIGatewayID
		runtime.ActiveISCSIGatewayID = gatewayID
		runtime.StandbyISCSIGatewayIDs = removeStringValue(runtime.StandbyISCSIGatewayIDs, gatewayID)
		if exportLeaseID := strings.TrimSpace(req.GetExportLeaseId()); exportLeaseID != "" {
			runtime.ExportLeaseID = exportLeaseID
		}
		if runtime.ExportLeaseID == "" {
			return status.Errorf(codes.FailedPrecondition, "iscsi export %q has no export lease", exportID)
		}
		volumeID = iscsiRegistryVolumeIDForExport(state, exportID)
		if volumeID == "" {
			return status.Errorf(codes.FailedPrecondition, "iscsi export %q has no volume mapping", exportID)
		}
		runtime.ExportEpoch = nextISCSIExportEpoch(runtime.ExportEpoch)
		runtime.State = "active"
		runtime.FailoverTrigger = firstNonEmptyISCSIString(strings.TrimSpace(req.GetTrigger()), "manual_promote")
		runtime.FailoverCompleted = true
		state.Failovers[exportID] = iscsi.NormalizeFailoverRuntime(runtime)
		return nil
	}, func(state *iscsiRegistryState) error {
		fence := service.ISCSIWriterFence{
			VolumeID: volumeID, ExportID: exportID, ExportLeaseID: runtime.ExportLeaseID,
			ExportEpoch: runtime.ExportEpoch, ActiveGatewayID: runtime.ActiveISCSIGatewayID,
			RegistryRevision: state.RegistryRevision,
		}
		if err := s.projectISCSIWriterFence(ctx, fence); err != nil {
			return status.Errorf(codes.FailedPrecondition, "project receiver writer fence: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if current := state.Failovers[exportID]; current.ExportID != "" {
		runtime = current
	}
	return &adminv1.PromoteISCSIFailoverResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Failover:         iscsiFailoverToProto(runtime),
	}, nil
}

func (s *server) DemoteISCSIFailover(ctx context.Context, req *adminv1.DemoteISCSIFailoverRequest) (*adminv1.DemoteISCSIFailoverResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	exportID := strings.TrimSpace(req.GetExportId())
	if exportID == "" {
		return nil, status.Error(codes.InvalidArgument, "export_id is required")
	}
	resourceKey := "failover/" + exportID
	var runtime iscsi.FailoverRuntime
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.failover.demote", resourceKey, "", "iscsi failover gateway demoted", func(state *iscsiRegistryState) error {
		runtime = iscsi.NormalizeFailoverRuntime(state.Failovers[exportID])
		if runtime.ExportID == "" {
			return status.Errorf(codes.NotFound, "iscsi failover runtime %q not found", exportID)
		}
		gatewayID := strings.TrimSpace(req.GetGatewayId())
		if gatewayID == "" {
			gatewayID = runtime.ActiveISCSIGatewayID
		}
		if gatewayID == "" {
			return status.Errorf(codes.FailedPrecondition, "iscsi export %q has no active gateway", exportID)
		}
		if runtime.ActiveISCSIGatewayID != gatewayID {
			return status.Errorf(codes.FailedPrecondition, "iscsi gateway %q is not active for export %q", gatewayID, exportID)
		}
		runtime.PreviousActiveGatewayID = runtime.ActiveISCSIGatewayID
		runtime.ActiveISCSIGatewayID = ""
		runtime = iscsi.AddStandbyGatewayID(runtime, gatewayID)
		runtime.ExportEpoch = nextISCSIExportEpoch(runtime.ExportEpoch)
		runtime.State = "demoted"
		runtime.FailoverTrigger = firstNonEmptyISCSIString(strings.TrimSpace(req.GetTrigger()), "manual_demote")
		runtime.FailoverCompleted = true
		state.Failovers[exportID] = iscsi.NormalizeFailoverRuntime(runtime)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if current := state.Failovers[exportID]; current.ExportID != "" {
		runtime = current
	}
	return &adminv1.DemoteISCSIFailoverResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Failover:         iscsiFailoverToProto(runtime),
	}, nil
}

func (s *server) StandbyISCSIFailover(ctx context.Context, req *adminv1.StandbyISCSIFailoverRequest) (*adminv1.StandbyISCSIFailoverResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	exportID, gatewayID, err := validateISCSIFailoverIDs(req.GetExportId(), req.GetGatewayId())
	if err != nil {
		return nil, err
	}
	resourceKey := "failover/" + exportID
	var runtime iscsi.FailoverRuntime
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.failover.standby", resourceKey, "", "iscsi failover standby gateway registered", func(state *iscsiRegistryState) error {
		runtime = iscsi.NormalizeFailoverRuntime(state.Failovers[exportID])
		if runtime.ExportID == "" {
			return status.Errorf(codes.NotFound, "iscsi failover runtime %q not found", exportID)
		}
		if runtime.ActiveISCSIGatewayID == gatewayID {
			return status.Errorf(codes.FailedPrecondition, "iscsi gateway %q is already active for export %q", gatewayID, exportID)
		}
		if runtime.StaleGatewayRevokedID == gatewayID {
			return status.Errorf(codes.FailedPrecondition, "iscsi gateway %q is revoked stale for export %q", gatewayID, exportID)
		}
		runtime = iscsi.AddStandbyGatewayID(runtime, gatewayID)
		runtime.State = firstNonEmptyISCSIString(runtime.State, "standby_registered")
		state.Failovers[exportID] = iscsi.NormalizeFailoverRuntime(runtime)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if current := state.Failovers[exportID]; current.ExportID != "" {
		runtime = current
	}
	return &adminv1.StandbyISCSIFailoverResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Failover:         iscsiFailoverToProto(runtime),
	}, nil
}

func (s *server) RevokeStaleISCSIFailover(ctx context.Context, req *adminv1.RevokeStaleISCSIFailoverRequest) (*adminv1.RevokeStaleISCSIFailoverResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	exportID, gatewayID, err := validateISCSIFailoverIDs(req.GetExportId(), req.GetGatewayId())
	if err != nil {
		return nil, err
	}
	resourceKey := "failover/" + exportID
	var runtime iscsi.FailoverRuntime
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.failover.revoke_stale", resourceKey, "", "iscsi failover stale gateway revoked", func(state *iscsiRegistryState) error {
		runtime = iscsi.NormalizeFailoverRuntime(state.Failovers[exportID])
		if runtime.ExportID == "" {
			return status.Errorf(codes.NotFound, "iscsi failover runtime %q not found", exportID)
		}
		if runtime.ActiveISCSIGatewayID == gatewayID {
			runtime.PreviousActiveGatewayID = runtime.ActiveISCSIGatewayID
			runtime.ActiveISCSIGatewayID = ""
		}
		runtime = iscsi.RemoveStandbyGatewayID(runtime, gatewayID)
		runtime.StaleGatewayRevokedID = gatewayID
		runtime.StaleGatewayRejected = true
		runtime.LastRejectedISCSIGatewayID = gatewayID
		runtime.LastWriteGatewayID = gatewayID
		runtime.LastWriteAdmitted = false
		runtime.LastWriteRejectionReason = "revoked_stale_gateway"
		runtime.LastWriteSCSIStatus = "check_condition"
		runtime.LastWriteSenseKey = "data_protect"
		runtime.ExportEpoch = nextISCSIExportEpoch(runtime.ExportEpoch)
		runtime.State = "stale_revoked"
		runtime.FailoverTrigger = firstNonEmptyISCSIString(strings.TrimSpace(req.GetTrigger()), "manual_revoke_stale")
		runtime.FailoverCompleted = true
		state.Failovers[exportID] = iscsi.NormalizeFailoverRuntime(runtime)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if current := state.Failovers[exportID]; current.ExportID != "" {
		runtime = current
	}
	return &adminv1.RevokeStaleISCSIFailoverResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Failover:         iscsiFailoverToProto(runtime),
	}, nil
}

func (s *server) CreateISCSIPortal(ctx context.Context, req *adminv1.CreateISCSIPortalRequest) (*adminv1.CreateISCSIPortalResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	portalID := strings.TrimSpace(req.GetPortalId())
	address := strings.TrimSpace(req.GetAddress())
	if portalID == "" || address == "" {
		return nil, status.Error(codes.InvalidArgument, "portal_id and address are required")
	}
	resourceKey := "portal/" + portalID
	var portal iscsi.Portal
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.portal.create", resourceKey, "", "iscsi portal created", func(state *iscsiRegistryState) error {
		if _, ok := state.Portals[portalID]; ok {
			return status.Errorf(codes.AlreadyExists, "iscsi portal %q already exists", portalID)
		}
		portal = iscsi.NormalizePortal(iscsi.Portal{
			PortalID:  portalID,
			Address:   address,
			GatewayID: strings.TrimSpace(req.GetGatewayId()),
			Enabled:   req.GetEnabled(),
		})
		state.Portals[portal.PortalID] = portal
		return nil
	})
	if err != nil {
		return nil, err
	}
	if portal.PortalID == "" {
		portal = state.Portals[portalID]
	}
	return &adminv1.CreateISCSIPortalResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Portal:           iscsiPortalToProto(portal),
	}, nil
}

func (s *server) DeleteISCSIPortal(ctx context.Context, req *adminv1.DeleteISCSIPortalRequest) (*adminv1.DeleteISCSIPortalResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	portalID := strings.TrimSpace(req.GetPortalId())
	if portalID == "" {
		return nil, status.Error(codes.InvalidArgument, "portal_id is required")
	}
	resourceKey := "portal/" + portalID
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.portal.delete", resourceKey, "", "iscsi portal deleted", func(state *iscsiRegistryState) error {
		if _, ok := state.Portals[portalID]; !ok {
			return status.Errorf(codes.NotFound, "iscsi portal %q not found", portalID)
		}
		for targetIQN, target := range state.Targets {
			target = iscsi.NormalizeTarget(target)
			if !iscsiStringSliceContains(target.PortalIDs, portalID) {
				continue
			}
			if !req.GetForce() {
				return status.Errorf(codes.FailedPrecondition, "iscsi portal %q is still referenced by target %q", portalID, targetIQN)
			}
			target.PortalIDs = removeStringValue(target.PortalIDs, portalID)
			if len(target.PortalIDs) == 0 {
				target.PortalID = ""
				target.Enabled = false
			} else {
				target.PortalID = target.PortalIDs[0]
			}
			state.Targets[targetIQN] = target
		}
		delete(state.Portals, portalID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &adminv1.DeleteISCSIPortalResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		PortalId:         portalID,
	}, nil
}

func (s *server) SetISCSIPortalEnabled(ctx context.Context, req *adminv1.SetISCSIPortalEnabledRequest) (*adminv1.SetISCSIPortalEnabledResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	portalID := strings.TrimSpace(req.GetPortalId())
	if portalID == "" {
		return nil, status.Error(codes.InvalidArgument, "portal_id is required")
	}
	resourceKey := "portal/" + portalID
	var portal iscsi.Portal
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.portal.set_enabled", resourceKey, "", "iscsi portal enabled state updated", func(state *iscsiRegistryState) error {
		var ok bool
		portal, ok = state.Portals[portalID]
		if !ok {
			return status.Errorf(codes.NotFound, "iscsi portal %q not found", portalID)
		}
		portal.Enabled = req.GetEnabled()
		state.Portals[portalID] = portal
		return nil
	})
	if err != nil {
		return nil, err
	}
	if portal.PortalID == "" {
		portal = state.Portals[portalID]
	}
	return &adminv1.SetISCSIPortalEnabledResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Portal:           iscsiPortalToProto(portal),
	}, nil
}

func (s *server) CreateISCSITarget(ctx context.Context, req *adminv1.CreateISCSITargetRequest) (*adminv1.CreateISCSITargetResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	portalIDs := uniqueTrimmedStrings(req.GetPortalIds())
	if targetIQN == "" || len(portalIDs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "target_iqn and at least one portal_id are required")
	}
	resourceKey := "target/" + targetIQN
	var target iscsi.Target
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.target.create", resourceKey, "", "iscsi target created", func(state *iscsiRegistryState) error {
		if _, ok := state.Targets[targetIQN]; ok {
			return status.Errorf(codes.AlreadyExists, "iscsi target %q already exists", targetIQN)
		}
		for _, portalID := range portalIDs {
			if _, ok := state.Portals[portalID]; !ok {
				return status.Errorf(codes.NotFound, "iscsi portal %q not found", portalID)
			}
		}
		exportID := strings.TrimSpace(req.GetExportId())
		if exportID == "" {
			exportID = targetIQN
		}
		target = iscsi.NormalizeTarget(iscsi.Target{
			TargetIQN: targetIQN,
			PortalID:  portalIDs[0],
			PortalIDs: portalIDs,
			ExportID:  exportID,
			Enabled:   req.GetEnabled(),
		})
		state.Targets[target.TargetIQN] = target
		return nil
	})
	if err != nil {
		return nil, err
	}
	if target.TargetIQN == "" {
		target = state.Targets[targetIQN]
	}
	return &adminv1.CreateISCSITargetResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Target:           iscsiTargetToProto(target),
	}, nil
}

func (s *server) DeleteISCSITarget(ctx context.Context, req *adminv1.DeleteISCSITargetRequest) (*adminv1.DeleteISCSITargetResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	if targetIQN == "" {
		return nil, status.Error(codes.InvalidArgument, "target_iqn is required")
	}
	resourceKey := "target/" + targetIQN
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.target.delete", resourceKey, "", "iscsi target deleted", func(state *iscsiRegistryState) error {
		if _, ok := state.Targets[targetIQN]; !ok {
			return status.Errorf(codes.NotFound, "iscsi target %q not found", targetIQN)
		}
		if !req.GetForce() {
			for _, lun := range state.LUNs {
				if lun.TargetIQN == targetIQN {
					return status.Errorf(codes.FailedPrecondition, "iscsi target %q still has exported LUNs", targetIQN)
				}
			}
			for _, session := range state.Sessions {
				if session.TargetIQN == targetIQN && session.Connected {
					return status.Errorf(codes.FailedPrecondition, "iscsi target %q still has connected sessions", targetIQN)
				}
			}
		}
		delete(state.Targets, targetIQN)
		for key, lun := range state.LUNs {
			if lun.TargetIQN == targetIQN {
				delete(state.LUNs, key)
			}
		}
		for key, acl := range state.ACLs {
			if acl.TargetIQN == targetIQN {
				delete(state.ACLs, key)
			}
		}
		for key, session := range state.Sessions {
			if session.TargetIQN == targetIQN {
				delete(state.Sessions, key)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &adminv1.DeleteISCSITargetResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		TargetIqn:        targetIQN,
	}, nil
}

func (s *server) SetISCSITargetEnabled(ctx context.Context, req *adminv1.SetISCSITargetEnabledRequest) (*adminv1.SetISCSITargetEnabledResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	if targetIQN == "" {
		return nil, status.Error(codes.InvalidArgument, "target_iqn is required")
	}
	resourceKey := "target/" + targetIQN
	var target iscsi.Target
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.target.set_enabled", resourceKey, "", "iscsi target enabled state updated", func(state *iscsiRegistryState) error {
		var ok bool
		target, ok = state.Targets[targetIQN]
		if !ok {
			return status.Errorf(codes.NotFound, "iscsi target %q not found", targetIQN)
		}
		target.Enabled = req.GetEnabled()
		state.Targets[targetIQN] = iscsi.NormalizeTarget(target)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if target.TargetIQN == "" {
		target = state.Targets[targetIQN]
	}
	return &adminv1.SetISCSITargetEnabledResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Target:           iscsiTargetToProto(target),
	}, nil
}

func (s *server) ExportISCSILUN(ctx context.Context, req *adminv1.ExportISCSILUNRequest) (*adminv1.ExportISCSILUNResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	if targetIQN == "" {
		return nil, status.Error(codes.InvalidArgument, "target_iqn is required")
	}
	volumeID, err := clustermeta.CanonicalVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume_id: %v", err)
	}
	if _, err := s.repo.GetVolumeState(ctx, volumeID); err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "get volume %q: %v", volumeID, err)
	}
	exportMode, err := normalizeISCSIExportMode(req.GetExportMode())
	if err != nil {
		return nil, err
	}
	blockSize := req.GetLogicalBlockSizeBytes()
	if blockSize == 0 {
		blockSize = 4096
	}
	exportID := strings.TrimSpace(req.GetExportId())
	if exportID == "" {
		exportID = fmt.Sprintf("%s-lun-%d", volumeID, req.GetLunId())
	}
	resourceKey := "lun/" + iscsi.LUNKey(targetIQN, req.GetLunId())
	var lun iscsi.LUN
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.lun.export", resourceKey, volumeID, "iscsi lun exported", func(state *iscsiRegistryState) error {
		if _, ok := state.Targets[targetIQN]; !ok {
			return status.Errorf(codes.NotFound, "iscsi target %q not found", targetIQN)
		}
		key := iscsi.LUNKey(targetIQN, req.GetLunId())
		if _, ok := state.LUNs[key]; ok {
			return status.Errorf(codes.AlreadyExists, "iscsi lun %q already exists", key)
		}
		lun = iscsi.LUN{
			TargetIQN:             targetIQN,
			LUNID:                 req.GetLunId(),
			LUNWWN:                iscsi.LUNWWN(exportID),
			ExportID:              exportID,
			VolumeID:              volumeID,
			ExportMode:            exportMode,
			LogicalBlockSizeBytes: blockSize,
			Enabled:               req.GetEnabled(),
		}
		if err := iscsi.ValidateExportVolumeLimit(state.controlState(), lun); err != nil {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
		state.LUNs[key] = lun
		if _, ok := state.Failovers[exportID]; !ok {
			state.Failovers[exportID] = iscsi.NormalizeFailoverRuntime(iscsi.FailoverRuntime{
				ExportID:       exportID,
				ExportEpoch:    1,
				State:          "registered",
				WriterPolicy:   iscsi.WriterPolicySingleActiveWriterSession,
				HAFailoverMode: iscsi.HAFailoverModeManualPromoteDemote,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if lun.TargetIQN == "" {
		lun = state.LUNs[iscsi.LUNKey(targetIQN, req.GetLunId())]
	}
	return &adminv1.ExportISCSILUNResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Lun:              iscsiLUNToProto(lun),
	}, nil
}

func (s *server) UnexportISCSILUN(ctx context.Context, req *adminv1.UnexportISCSILUNRequest) (*adminv1.UnexportISCSILUNResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	if targetIQN == "" {
		return nil, status.Error(codes.InvalidArgument, "target_iqn is required")
	}
	key := iscsi.LUNKey(targetIQN, req.GetLunId())
	resourceKey := "lun/" + key
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.lun.unexport", resourceKey, "", "iscsi lun unexported", func(state *iscsiRegistryState) error {
		lun, ok := state.LUNs[key]
		if !ok {
			return status.Errorf(codes.NotFound, "iscsi lun %q not found", key)
		}
		for sessionKey, session := range state.Sessions {
			if session.TargetIQN != targetIQN || session.LUNID != req.GetLunId() {
				continue
			}
			if session.Connected && !req.GetForce() {
				return status.Errorf(codes.FailedPrecondition, "iscsi lun %q still has connected session %q", key, session.SessionID)
			}
			if req.GetForce() {
				delete(state.Sessions, sessionKey)
			}
		}
		delete(state.LUNs, key)
		delete(state.Failovers, lun.ExportID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &adminv1.UnexportISCSILUNResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		TargetIqn:        targetIQN,
		LunId:            req.GetLunId(),
	}, nil
}

func (s *server) SetISCSILUNMode(ctx context.Context, req *adminv1.SetISCSILUNModeRequest) (*adminv1.SetISCSILUNModeResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	if targetIQN == "" {
		return nil, status.Error(codes.InvalidArgument, "target_iqn is required")
	}
	exportMode, err := normalizeISCSIExportMode(req.GetExportMode())
	if err != nil {
		return nil, err
	}
	key := iscsi.LUNKey(targetIQN, req.GetLunId())
	resourceKey := "lun/" + key
	var lun iscsi.LUN
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.lun.set_mode", resourceKey, "", "iscsi lun export mode updated", func(state *iscsiRegistryState) error {
		var ok bool
		lun, ok = state.LUNs[key]
		if !ok {
			return status.Errorf(codes.NotFound, "iscsi lun %q not found", key)
		}
		lun.ExportMode = exportMode
		state.LUNs[key] = lun
		return nil
	})
	if err != nil {
		return nil, err
	}
	if lun.TargetIQN == "" {
		lun = state.LUNs[key]
	}
	return &adminv1.SetISCSILUNModeResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Lun:              iscsiLUNToProto(lun),
	}, nil
}

func (s *server) AllowISCSIInitiator(ctx context.Context, req *adminv1.AllowISCSIInitiatorRequest) (*adminv1.AllowISCSIInitiatorResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	initiatorIQN := strings.TrimSpace(req.GetInitiatorIqn())
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	if initiatorIQN == "" || targetIQN == "" {
		return nil, status.Error(codes.InvalidArgument, "initiator_iqn and target_iqn are required")
	}
	allowedLUNs := uniqueUint64s(req.GetAllowedLunIds())
	if len(allowedLUNs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one allowed_lun_id is required")
	}
	authMode, chapSecretSet, err := normalizeISCSIAuthMode(req.GetAuthMode(), req.GetChapSecretRef())
	if err != nil {
		return nil, err
	}
	key := iscsi.ACLKey(initiatorIQN, targetIQN)
	resourceKey := "initiator_acl/" + key
	var acl iscsi.InitiatorACL
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.initiator.allow", resourceKey, "", "iscsi initiator allowed", func(state *iscsiRegistryState) error {
		if _, ok := state.Targets[targetIQN]; !ok {
			return status.Errorf(codes.NotFound, "iscsi target %q not found", targetIQN)
		}
		for _, lunID := range allowedLUNs {
			if _, ok := state.LUNs[iscsi.LUNKey(targetIQN, lunID)]; !ok {
				return status.Errorf(codes.NotFound, "iscsi lun %s not found", iscsi.LUNKey(targetIQN, lunID))
			}
		}
		acl = iscsi.InitiatorACL{
			InitiatorIQN:  initiatorIQN,
			TargetIQN:     targetIQN,
			AllowedLUNs:   allowedLUNs,
			AuthMode:      authMode,
			CHAPSecretSet: chapSecretSet,
			CHAPSecretRef: strings.TrimSpace(req.GetChapSecretRef()),
			Enabled:       req.GetEnabled(),
		}
		state.ACLs[key] = acl
		return nil
	})
	if err != nil {
		return nil, err
	}
	if acl.InitiatorIQN == "" {
		acl = state.ACLs[key]
	}
	return &adminv1.AllowISCSIInitiatorResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		InitiatorAcl:     iscsiACLToProto(acl),
	}, nil
}

func (s *server) DenyISCSIInitiator(ctx context.Context, req *adminv1.DenyISCSIInitiatorRequest) (*adminv1.DenyISCSIInitiatorResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	initiatorIQN := strings.TrimSpace(req.GetInitiatorIqn())
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	if initiatorIQN == "" || targetIQN == "" {
		return nil, status.Error(codes.InvalidArgument, "initiator_iqn and target_iqn are required")
	}
	key := iscsi.ACLKey(initiatorIQN, targetIQN)
	resourceKey := "initiator_acl/" + key
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.initiator.deny", resourceKey, "", "iscsi initiator denied", func(state *iscsiRegistryState) error {
		if _, ok := state.ACLs[key]; !ok {
			return status.Errorf(codes.NotFound, "iscsi initiator acl %q not found", key)
		}
		delete(state.ACLs, key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &adminv1.DenyISCSIInitiatorResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		InitiatorIqn:     initiatorIQN,
		TargetIqn:        targetIQN,
	}, nil
}

func (s *server) SetISCSIInitiatorAuth(ctx context.Context, req *adminv1.SetISCSIInitiatorAuthRequest) (*adminv1.SetISCSIInitiatorAuthResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	initiatorIQN := strings.TrimSpace(req.GetInitiatorIqn())
	targetIQN := strings.TrimSpace(req.GetTargetIqn())
	if initiatorIQN == "" || targetIQN == "" {
		return nil, status.Error(codes.InvalidArgument, "initiator_iqn and target_iqn are required")
	}
	authMode, chapSecretSet, err := normalizeISCSIAuthMode(req.GetAuthMode(), req.GetChapSecretRef())
	if err != nil {
		return nil, err
	}
	key := iscsi.ACLKey(initiatorIQN, targetIQN)
	resourceKey := "initiator_acl/" + key
	var acl iscsi.InitiatorACL
	op, state, err := s.mutateISCSIRegistry(ctx, req.GetMeta(), req.GetIdempotencyKey(), req.GetExpectedRegistryRevision(), "iscsi.initiator.set_auth", resourceKey, "", "iscsi initiator auth updated", func(state *iscsiRegistryState) error {
		var ok bool
		acl, ok = state.ACLs[key]
		if !ok {
			return status.Errorf(codes.NotFound, "iscsi initiator acl %q not found", key)
		}
		acl.AuthMode = authMode
		acl.CHAPSecretSet = chapSecretSet
		acl.CHAPSecretRef = strings.TrimSpace(req.GetChapSecretRef())
		state.ACLs[key] = acl
		return nil
	})
	if err != nil {
		return nil, err
	}
	if acl.InitiatorIQN == "" {
		acl = state.ACLs[key]
	}
	return &adminv1.SetISCSIInitiatorAuthResponse{
		Cluster:          cluster,
		Operation:        op,
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		InitiatorAcl:     iscsiACLToProto(acl),
	}, nil
}

func (s *server) mutateISCSIRegistry(ctx context.Context, meta *adminv1.RequestMeta, idempotencyKey string, expectedRevision uint64, kind, resourceKey, volumeID, message string, mutate func(*iscsiRegistryState) error) (*adminv1.OperationHandle, *iscsiRegistryState, error) {
	return s.mutateISCSIRegistryWithPrecommit(ctx, meta, idempotencyKey, expectedRevision, kind, resourceKey, volumeID, message, mutate, nil)
}

func (s *server) mutateISCSIRegistryWithPrecommit(ctx context.Context, meta *adminv1.RequestMeta, idempotencyKey string, expectedRevision uint64, kind, resourceKey, volumeID, message string, mutate func(*iscsiRegistryState) error, precommit func(*iscsiRegistryState) error) (*adminv1.OperationHandle, *iscsiRegistryState, error) {
	actor := strings.TrimSpace(meta.GetActor())
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	kind = strings.TrimSpace(kind)
	resourceKey = strings.TrimSpace(resourceKey)
	if actor == "" {
		return nil, nil, status.Error(codes.InvalidArgument, "request meta actor is required")
	}
	if idempotencyKey == "" {
		return nil, nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	if kind == "" || resourceKey == "" {
		return nil, nil, status.Error(codes.Internal, "iscsi mutation kind and resource key are required")
	}
	if err := enforceDependencyISCSIRegistryMutation(kind); err != nil {
		return nil, nil, err
	}

	s.iscsiMu.Lock()
	defer s.iscsiMu.Unlock()

	state, err := s.loadISCSIRegistryState(ctx)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "load iscsi registry: %v", err)
	}
	if record, ok := state.IdempotencyRecords[idempotencyKey]; ok {
		if record.Kind != kind || record.ResourceKey != resourceKey {
			return nil, nil, status.Errorf(codes.FailedPrecondition, "idempotency_key %q was already used for %s %s", idempotencyKey, record.Kind, record.ResourceKey)
		}
		return &adminv1.OperationHandle{
			Accepted:    false,
			OperationId: record.OperationID,
			Message:     "idempotent replay: " + record.Message,
		}, state, nil
	}
	if expectedRevision != 0 && expectedRevision != state.RegistryRevision {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "registry revision %d does not match expected %d", state.RegistryRevision, expectedRevision)
	}
	before, err := cloneISCSIRegistryState(state)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "snapshot iscsi registry before mutation: %v", err)
	}
	if err := mutate(state); err != nil {
		return nil, nil, err
	}
	state.RegistryRevision++
	state.ConfigGeneration++
	state.UpdatedAtUnix = time.Now().UTC().Unix()
	if precommit != nil {
		if err := precommit(state); err != nil {
			return nil, nil, err
		}
	}
	op, err := s.createISCSIRegistryOperation(kind, volumeID)
	if err != nil {
		return nil, nil, err
	}
	state.IdempotencyRecords[idempotencyKey] = iscsiRegistryIdempotency{
		Kind:             kind,
		ResourceKey:      resourceKey,
		OperationID:      op.GetOperationId(),
		RegistryRevision: state.RegistryRevision,
		ConfigGeneration: state.ConfigGeneration,
		Message:          message,
	}
	if err := s.saveISCSIRegistryStateDelta(ctx, before, state); err != nil {
		return nil, nil, status.Errorf(codes.Internal, "save iscsi registry: %v", err)
	}
	return acceptedOperation(op, message), state, nil
}

func iscsiRegistryVolumeIDForExport(state *iscsiRegistryState, exportID string) string {
	for _, lun := range state.LUNs {
		if strings.TrimSpace(lun.ExportID) == strings.TrimSpace(exportID) {
			return strings.TrimSpace(lun.VolumeID)
		}
	}
	return ""
}

func (s *server) createISCSIRegistryOperation(kind, volumeID string) (*adminv1.OperationStatus, error) {
	if s.ops == nil {
		return nil, nil
	}
	op, err := s.ops.create(kind, "", volumeID, "completed", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record iscsi operation: %v", err)
	}
	return op, nil
}

func normalizeISCSIExportMode(raw string) (string, error) {
	mode := strings.TrimSpace(raw)
	if mode == "" {
		mode = "read_write"
	}
	switch mode {
	case "read_write", "read_only":
		return mode, nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "unsupported export_mode %q", mode)
	}
}

func normalizeISCSIAuthMode(raw, chapSecretRef string) (string, bool, error) {
	mode := strings.TrimSpace(raw)
	if mode == "" {
		mode = "none"
	}
	switch mode {
	case "none":
		return mode, false, nil
	case "chap":
		if strings.TrimSpace(chapSecretRef) == "" {
			return "", false, status.Error(codes.InvalidArgument, "chap_secret_ref is required for chap auth")
		}
		return mode, true, nil
	default:
		return "", false, status.Errorf(codes.InvalidArgument, "unsupported auth_mode %q", mode)
	}
}

func validateISCSIFailoverIDs(exportID, gatewayID string) (string, string, error) {
	exportID = strings.TrimSpace(exportID)
	gatewayID = strings.TrimSpace(gatewayID)
	if exportID == "" || gatewayID == "" {
		return "", "", status.Error(codes.InvalidArgument, "export_id and gateway_id are required")
	}
	return exportID, gatewayID, nil
}

func iscsiRegistryExportExists(state *iscsiRegistryState, exportID string) bool {
	if state == nil {
		return false
	}
	exportID = strings.TrimSpace(exportID)
	if exportID == "" {
		return false
	}
	if runtime := state.Failovers[exportID]; runtime.ExportID != "" {
		return true
	}
	for _, lun := range state.LUNs {
		if lun.ExportID == exportID {
			return true
		}
	}
	return false
}

func nextISCSIExportEpoch(current uint64) uint64 {
	if current == 0 {
		return 1
	}
	return current + 1
}

func firstNonEmptyISCSIString(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func iscsiPortalSummaries(state *iscsi.ControlState) []*adminv1.ISCSIPortalSummary {
	state = state.Normalize()
	keys := sortedMapKeys(state.Portals)
	out := make([]*adminv1.ISCSIPortalSummary, 0, len(keys))
	for _, key := range keys {
		out = append(out, iscsiPortalToProto(state.Portals[key]))
	}
	return out
}

func iscsiTargetSummaries(state *iscsi.ControlState) []*adminv1.ISCSITargetSummary {
	state = state.Normalize()
	keys := sortedMapKeys(state.Targets)
	out := make([]*adminv1.ISCSITargetSummary, 0, len(keys))
	for _, key := range keys {
		out = append(out, iscsiTargetToProto(state.Targets[key]))
	}
	return out
}

func iscsiLUNSummaries(state *iscsi.ControlState, targetIQN string) []*adminv1.ISCSILUNSummary {
	state = state.Normalize()
	targetIQN = strings.TrimSpace(targetIQN)
	keys := sortedMapKeys(state.LUNs)
	out := make([]*adminv1.ISCSILUNSummary, 0, len(keys))
	for _, key := range keys {
		lun := state.LUNs[key]
		if targetIQN != "" && lun.TargetIQN != targetIQN {
			continue
		}
		out = append(out, iscsiLUNToProto(lun))
	}
	return out
}

func iscsiACLSummaries(state *iscsi.ControlState, targetIQN string) []*adminv1.ISCSIInitiatorACLSummary {
	state = state.Normalize()
	targetIQN = strings.TrimSpace(targetIQN)
	keys := sortedMapKeys(state.ACLs)
	out := make([]*adminv1.ISCSIInitiatorACLSummary, 0, len(keys))
	for _, key := range keys {
		acl := state.ACLs[key]
		if targetIQN != "" && acl.TargetIQN != targetIQN {
			continue
		}
		out = append(out, iscsiACLToProto(acl))
	}
	return out
}

func iscsiSessionSummaries(state *iscsi.ControlState, targetIQN, initiatorIQN string, connectedOnly bool) []*adminv1.ISCSISessionSummary {
	state = state.Normalize()
	targetIQN = strings.TrimSpace(targetIQN)
	initiatorIQN = strings.TrimSpace(initiatorIQN)
	keys := sortedMapKeys(state.Sessions)
	out := make([]*adminv1.ISCSISessionSummary, 0, len(keys))
	for _, key := range keys {
		session := iscsi.NormalizeSession(state.Sessions[key])
		if targetIQN != "" && session.TargetIQN != targetIQN {
			continue
		}
		if initiatorIQN != "" && session.InitiatorIQN != initiatorIQN {
			continue
		}
		if connectedOnly && !session.Connected {
			continue
		}
		out = append(out, iscsiSessionToProto(session))
	}
	return out
}

func iscsiFailoverSummaries(state *iscsi.ControlState) []*adminv1.ISCSIFailoverRuntimeSummary {
	state = state.Normalize()
	keys := sortedMapKeys(state.Failovers)
	out := make([]*adminv1.ISCSIFailoverRuntimeSummary, 0, len(keys))
	for _, key := range keys {
		out = append(out, iscsiFailoverToProto(state.Failovers[key]))
	}
	return out
}

func iscsiPortalToProto(portal iscsi.Portal) *adminv1.ISCSIPortalSummary {
	portal = iscsi.NormalizePortal(portal)
	return &adminv1.ISCSIPortalSummary{
		PortalId:  portal.PortalID,
		Address:   portal.Address,
		GatewayId: portal.GatewayID,
		Enabled:   portal.Enabled,
	}
}

func iscsiTargetToProto(target iscsi.Target) *adminv1.ISCSITargetSummary {
	target = iscsi.NormalizeTarget(target)
	return &adminv1.ISCSITargetSummary{
		TargetIqn: target.TargetIQN,
		PortalId:  target.PortalID,
		PortalIds: append([]string{}, target.PortalIDs...),
		ExportId:  target.ExportID,
		Enabled:   target.Enabled,
	}
}

func iscsiLUNToProto(lun iscsi.LUN) *adminv1.ISCSILUNSummary {
	if strings.TrimSpace(lun.LUNWWN) == "" {
		lun.LUNWWN = iscsi.LUNWWN(lun.ExportID)
	}
	return &adminv1.ISCSILUNSummary{
		TargetIqn:             strings.TrimSpace(lun.TargetIQN),
		LunId:                 lun.LUNID,
		LunWwn:                strings.TrimSpace(lun.LUNWWN),
		ExportId:              strings.TrimSpace(lun.ExportID),
		VolumeId:              strings.TrimSpace(lun.VolumeID),
		ExportMode:            strings.TrimSpace(lun.ExportMode),
		LogicalBlockSizeBytes: lun.LogicalBlockSizeBytes,
		Enabled:               lun.Enabled,
	}
}

func iscsiACLToProto(acl iscsi.InitiatorACL) *adminv1.ISCSIInitiatorACLSummary {
	return &adminv1.ISCSIInitiatorACLSummary{
		InitiatorIqn:  strings.TrimSpace(acl.InitiatorIQN),
		TargetIqn:     strings.TrimSpace(acl.TargetIQN),
		AllowedLunIds: append([]uint64{}, acl.AllowedLUNs...),
		AuthMode:      strings.TrimSpace(acl.AuthMode),
		ChapSecretSet: acl.CHAPSecretSet,
		ChapSecretRef: strings.TrimSpace(acl.CHAPSecretRef),
		Enabled:       acl.Enabled,
	}
}

func iscsiSessionToProto(session iscsi.Session) *adminv1.ISCSISessionSummary {
	session = iscsi.NormalizeSession(session)
	return &adminv1.ISCSISessionSummary{
		SessionId:            session.SessionID,
		TargetIqn:            session.TargetIQN,
		InitiatorIqn:         session.InitiatorIQN,
		LunId:                session.LUNID,
		LunWwn:               session.LUNWWN,
		IscsiGatewayId:       session.ISCSIGatewayID,
		State:                session.State,
		Connected:            session.Connected,
		IscsiErl:             int32(session.ISCSIERL),
		ConnectionCount:      int32(session.ConnectionCount),
		HeaderDigest:         session.HeaderDigest,
		DataDigest:           session.DataDigest,
		WriterPolicy:         session.WriterPolicy,
		HaFailoverMode:       session.HAFailoverMode,
		ActiveIscsiGatewayId: session.ActiveGatewayID,
		ExportEpoch:          session.ExportEpoch,
		ReadWriteAllowed:     session.ReadWriteAllowed,
		ScsiStatus:           session.SCSIStatus,
		SenseKey:             session.SenseKey,
		LastErrorClass:       session.LastErrorClass,
		LastError:            session.LastError,
		BytesRead:            session.BytesRead,
		BytesWritten:         session.BytesWritten,
		FlushCount:           session.FlushCount,
		UnmapBytes:           session.UnmapBytes,
	}
}

func iscsiSessionFromProto(session *adminv1.ISCSISessionSummary) (iscsi.Session, error) {
	if session == nil {
		return iscsi.Session{}, status.Error(codes.InvalidArgument, "session is required")
	}
	out := iscsi.NormalizeSession(iscsi.Session{
		SessionID:        strings.TrimSpace(session.GetSessionId()),
		TargetIQN:        strings.TrimSpace(session.GetTargetIqn()),
		InitiatorIQN:     strings.TrimSpace(session.GetInitiatorIqn()),
		LUNID:            session.GetLunId(),
		LUNWWN:           strings.TrimSpace(session.GetLunWwn()),
		ISCSIGatewayID:   strings.TrimSpace(session.GetIscsiGatewayId()),
		State:            strings.TrimSpace(session.GetState()),
		Connected:        session.GetConnected(),
		ISCSIERL:         int(session.GetIscsiErl()),
		ConnectionCount:  int(session.GetConnectionCount()),
		HeaderDigest:     strings.TrimSpace(session.GetHeaderDigest()),
		DataDigest:       strings.TrimSpace(session.GetDataDigest()),
		WriterPolicy:     strings.TrimSpace(session.GetWriterPolicy()),
		HAFailoverMode:   strings.TrimSpace(session.GetHaFailoverMode()),
		ActiveGatewayID:  strings.TrimSpace(session.GetActiveIscsiGatewayId()),
		ExportEpoch:      session.GetExportEpoch(),
		ReadWriteAllowed: session.GetReadWriteAllowed(),
		SCSIStatus:       strings.TrimSpace(session.GetScsiStatus()),
		SenseKey:         strings.TrimSpace(session.GetSenseKey()),
		LastErrorClass:   strings.TrimSpace(session.GetLastErrorClass()),
		LastError:        strings.TrimSpace(session.GetLastError()),
		BytesRead:        session.GetBytesRead(),
		BytesWritten:     session.GetBytesWritten(),
		FlushCount:       session.GetFlushCount(),
		UnmapBytes:       session.GetUnmapBytes(),
	})
	if out.SessionID == "" || out.TargetIQN == "" || out.InitiatorIQN == "" {
		return iscsi.Session{}, status.Error(codes.InvalidArgument, "session_id, target_iqn, and initiator_iqn are required")
	}
	return out, nil
}

func iscsiFailoverToProto(runtime iscsi.FailoverRuntime) *adminv1.ISCSIFailoverRuntimeSummary {
	runtime = iscsi.NormalizeFailoverRuntime(runtime)
	return &adminv1.ISCSIFailoverRuntimeSummary{
		ExportId:                     runtime.ExportID,
		ActiveIscsiGatewayId:         runtime.ActiveISCSIGatewayID,
		StandbyIscsiGatewayIds:       append([]string{}, runtime.StandbyISCSIGatewayIDs...),
		PreviousActiveIscsiGatewayId: runtime.PreviousActiveGatewayID,
		ExportLeaseId:                runtime.ExportLeaseID,
		ExportEpoch:                  runtime.ExportEpoch,
		State:                        runtime.State,
		WriterPolicy:                 runtime.WriterPolicy,
		HaFailoverMode:               runtime.HAFailoverMode,
		AluaMode:                     runtime.ALUAMode,
		AluaImplicitSupported:        runtime.ALUAImplicitSupported,
		AluaExplicitSupported:        runtime.ALUAExplicitSupported,
		ActiveAluaAccessState:        runtime.ActiveALUAAccessState,
		StandbyAluaAccessState:       runtime.StandbyALUAAccessState,
		FailoverTrigger:              runtime.FailoverTrigger,
		FailoverCompleted:            runtime.FailoverCompleted,
		StaleGatewayRevokedId:        runtime.StaleGatewayRevokedID,
		StaleGatewayRejected:         runtime.StaleGatewayRejected,
		StandbyWriteRejected:         runtime.StandbyWriteRejected,
		LastWriteGatewayId:           runtime.LastWriteGatewayID,
		LastWriteObservedEpoch:       runtime.LastWriteObservedEpoch,
		LastWriteAdmitted:            runtime.LastWriteAdmitted,
		LastWriteRejectionReason:     runtime.LastWriteRejectionReason,
		LastWriteScsiStatus:          runtime.LastWriteSCSIStatus,
		LastWriteSenseKey:            runtime.LastWriteSenseKey,
		LastRejectedIscsiGatewayId:   runtime.LastRejectedISCSIGatewayID,
	}
}

func iscsiObservabilityCountersToProto(counters iscsi.ObservabilityCounters) *adminv1.ISCSIObservabilityCounters {
	return &adminv1.ISCSIObservabilityCounters{
		SessionCount:               uint32(counters.SessionCount),
		ConnectedSessions:          uint32(counters.ConnectedSessions),
		ActiveSessions:             uint32(counters.ActiveSessions),
		ProtocolErrors:             counters.ProtocolErrors,
		BackendErrors:              counters.BackendErrors,
		AuthErrors:                 counters.AuthErrors,
		FencingErrors:              counters.FencingErrors,
		StaleRejects:               counters.StaleRejects,
		StandbyRejects:             counters.StandbyRejects,
		FlushCount:                 counters.FlushCount,
		UnmapBytes:                 counters.UnmapBytes,
		BytesRead:                  counters.BytesRead,
		BytesWritten:               counters.BytesWritten,
		LastRejectedIscsiGatewayId: strings.TrimSpace(counters.LastRejectedGatewayID),
		LastErrorClass:             strings.TrimSpace(counters.LastErrorClass),
		LastError:                  strings.TrimSpace(counters.LastError),
	}
}

func sortedMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func uniqueTrimmedStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func uniqueUint64s(values []uint64) []uint64 {
	seen := map[uint64]bool{}
	out := make([]uint64, 0, len(values))
	for _, value := range values {
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func iscsiStringSliceContains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func removeStringValue(values []string, remove string) []string {
	out := values[:0]
	for _, value := range values {
		if value != remove {
			out = append(out, value)
		}
	}
	return out
}

func iscsiRegistryNotFound(err error) bool {
	return err == clustermeta.ErrNotFound
}
