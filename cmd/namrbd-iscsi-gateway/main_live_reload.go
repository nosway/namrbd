package main

import (
	"context"
	"fmt"

	"github.com/nosway/namrbd/iscsi"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"google.golang.org/grpc"
)

type iscsiGatewayReloadClient interface {
	GetISCSIRegistryChanges(context.Context, *adminv1.GetISCSIRegistryChangesRequest, ...grpc.CallOption) (*adminv1.GetISCSIRegistryChangesResponse, error)
	ListISCSIExports(context.Context, *adminv1.ListISCSIExportsRequest, ...grpc.CallOption) (*adminv1.ListISCSIExportsResponse, error)
}

type grpcISCSIRegistryReloadSource struct {
	client iscsiGatewayReloadClient
}

func (s grpcISCSIRegistryReloadSource) GetChanges(ctx context.Context, afterRevision uint64, pageSize int, pageToken string) (iscsi.RegistryChangePage, error) {
	if s.client == nil {
		return iscsi.RegistryChangePage{}, fmt.Errorf("iSCSI registry reload client is required")
	}
	if pageSize < 1 || pageSize > iscsi.RegistryReloadPageSize {
		return iscsi.RegistryChangePage{}, fmt.Errorf("changed-set page size must be 1..%d, got %d", iscsi.RegistryReloadPageSize, pageSize)
	}
	resp, err := s.client.GetISCSIRegistryChanges(ctx, &adminv1.GetISCSIRegistryChangesRequest{
		AfterRevision: afterRevision,
		PageSize:      uint32(pageSize),
		PageToken:     pageToken,
	})
	if err != nil {
		return iscsi.RegistryChangePage{}, err
	}
	if err := validateReloadRegistryBoundary(resp.GetServingRegistryAuthority(), resp.GetStorageLayout()); err != nil {
		return iscsi.RegistryChangePage{}, err
	}
	page := iscsi.RegistryChangePage{
		FromRevision:        resp.GetFromRevision(),
		ToRevision:          resp.GetToRevision(),
		ConfigGeneration:    resp.GetConfigGeneration(),
		NextPageToken:       resp.GetNextPageToken(),
		CheckpointRevision:  resp.GetCheckpointRevision(),
		ResyncRequired:      resp.GetResyncRequired(),
		ResyncReason:        resp.GetResyncReason(),
		ChangeFloorRevision: resp.GetChangeFloorRevision(),
		Changes:             make([]iscsi.RegistryExportChange, 0, len(resp.GetChanges())),
	}
	for _, change := range resp.GetChanges() {
		converted := iscsi.RegistryExportChange{
			RegistryRevision: change.GetRegistryRevision(),
			ConfigGeneration: change.GetConfigGeneration(),
			Operation:        change.GetOperation(),
			ExportID:         change.GetExportId(),
		}
		if change.GetExport() != nil {
			export := registryExportStateFromProto(change.GetExport())
			converted.Export = &export
		}
		page.Changes = append(page.Changes, converted)
	}
	return page, nil
}

func (s grpcISCSIRegistryReloadSource) ListSnapshot(ctx context.Context, revision uint64, pageSize int, pageToken string) (iscsi.RegistrySnapshotPage, error) {
	if s.client == nil {
		return iscsi.RegistrySnapshotPage{}, fmt.Errorf("iSCSI registry reload client is required")
	}
	if pageSize < 1 || pageSize > iscsi.RegistryReloadPageSize {
		return iscsi.RegistrySnapshotPage{}, fmt.Errorf("snapshot page size must be 1..%d, got %d", iscsi.RegistryReloadPageSize, pageSize)
	}
	resp, err := s.client.ListISCSIExports(ctx, &adminv1.ListISCSIExportsRequest{
		RegistryRevision: revision,
		PageSize:         uint32(pageSize),
		PageToken:        pageToken,
	})
	if err != nil {
		return iscsi.RegistrySnapshotPage{}, err
	}
	if err := validateReloadRegistryBoundary(resp.GetServingRegistryAuthority(), resp.GetStorageLayout()); err != nil {
		return iscsi.RegistrySnapshotPage{}, err
	}
	page := iscsi.RegistrySnapshotPage{
		RegistryRevision: resp.GetRegistryRevision(),
		ConfigGeneration: resp.GetConfigGeneration(),
		NextPageToken:    resp.GetNextPageToken(),
		Exports:          make([]iscsi.RegistryExportState, 0, len(resp.GetExports())),
	}
	for _, export := range resp.GetExports() {
		page.Exports = append(page.Exports, registryExportStateFromProto(export))
	}
	return page, nil
}

func registryExportStateFromProto(export *adminv1.ISCSIExportSummary) iscsi.RegistryExportState {
	if export == nil {
		return iscsi.RegistryExportState{}
	}
	writeState := "standby"
	if export.GetReadWriteAllowed() {
		writeState = "read_write"
	}
	return iscsi.RegistryExportState{
		ExportID:            export.GetExportId(),
		VolumeID:            export.GetVolumeId(),
		TargetIQN:           export.GetTargetIqn(),
		LUNID:               export.GetLunId(),
		LUNWWN:              export.GetLunWwn(),
		Enabled:             export.GetEnabled(),
		ActiveGatewayID:     export.GetActiveIscsiGatewayId(),
		StandbyGatewayIDs:   append([]string(nil), export.GetStandbyIscsiGatewayIds()...),
		ExportLeaseID:       export.GetExportLeaseId(),
		ExportEpoch:         export.GetExportEpoch(),
		ReadWriteAllowed:    export.GetReadWriteAllowed(),
		WriteAdmissionState: writeState,
	}
}

func validateReloadRegistryBoundary(authority, layout string) error {
	if authority != iscsiGatewayRegistryAuthority {
		return fmt.Errorf("iSCSI serving registry authority is %q, want %q", authority, iscsiGatewayRegistryAuthority)
	}
	if layout != iscsiGatewayRegistryLayout {
		return fmt.Errorf("iSCSI registry storage layout is %q, want %q", layout, iscsiGatewayRegistryLayout)
	}
	return nil
}
