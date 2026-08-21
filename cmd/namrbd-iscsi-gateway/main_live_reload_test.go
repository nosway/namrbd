package main

import (
	"context"
	"testing"

	"github.com/nosway/namrbd/iscsi"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"google.golang.org/grpc"
)

func TestGRPCISCSIRegistryReloadSourceBindsBoundedChangedSetAndSnapshot(t *testing.T) {
	client := &fakeReloadClient{}
	source := grpcISCSIRegistryReloadSource{client: client}
	changes, err := source.GetChanges(context.Background(), 7, 128, "change-token")
	if err != nil {
		t.Fatal(err)
	}
	if client.changeRequest.GetAfterRevision() != 7 || client.changeRequest.GetPageSize() != 128 || client.changeRequest.GetPageToken() != "change-token" {
		t.Fatalf("changed-set request = %#v", client.changeRequest)
	}
	if changes.CheckpointRevision != 8 || len(changes.Changes) != 1 || changes.Changes[0].Export == nil || changes.Changes[0].Export.VolumeID != "volume-a" {
		t.Fatalf("changed-set page = %#v", changes)
	}
	if !changes.Changes[0].Export.ReadWriteAllowed || changes.Changes[0].Export.WriteAdmissionState != "read_write" {
		t.Fatalf("write state was not preserved: %#v", changes.Changes[0].Export)
	}

	snapshot, err := source.ListSnapshot(context.Background(), 8, 128, "snapshot-token")
	if err != nil {
		t.Fatal(err)
	}
	if client.snapshotRequest.GetRegistryRevision() != 8 || client.snapshotRequest.GetPageSize() != 128 || client.snapshotRequest.GetPageToken() != "snapshot-token" {
		t.Fatalf("snapshot request = %#v", client.snapshotRequest)
	}
	if snapshot.RegistryRevision != 8 || len(snapshot.Exports) != 1 || snapshot.Exports[0].ExportID != "export-a" {
		t.Fatalf("snapshot page = %#v", snapshot)
	}
}

func TestGRPCISCSIRegistryReloadSourceRejectsUnboundedPage(t *testing.T) {
	source := grpcISCSIRegistryReloadSource{client: &fakeReloadClient{}}
	if _, err := source.GetChanges(context.Background(), 0, iscsi.RegistryReloadPageSize+1, ""); err == nil {
		t.Fatal("changed-set source accepted an unbounded page")
	}
	if _, err := source.ListSnapshot(context.Background(), 0, 0, ""); err == nil {
		t.Fatal("snapshot source accepted an empty page size")
	}
}

type fakeReloadClient struct {
	changeRequest   *adminv1.GetISCSIRegistryChangesRequest
	snapshotRequest *adminv1.ListISCSIExportsRequest
}

func (f *fakeReloadClient) GetISCSIRegistryChanges(_ context.Context, req *adminv1.GetISCSIRegistryChangesRequest, _ ...grpc.CallOption) (*adminv1.GetISCSIRegistryChangesResponse, error) {
	f.changeRequest = req
	export := fakeReloadExport()
	return &adminv1.GetISCSIRegistryChangesResponse{
		FromRevision: 7, ToRevision: 8, ConfigGeneration: 3,
		Changes: []*adminv1.ISCSIRegistryExportChange{{
			RegistryRevision: 8, ConfigGeneration: 3, Operation: "upsert",
			ExportId: "export-a", Export: export,
		}},
		CheckpointRevision:       8,
		ServingRegistryAuthority: iscsiGatewayRegistryAuthority,
		StorageLayout:            iscsiGatewayRegistryLayout,
	}, nil
}

func (f *fakeReloadClient) ListISCSIExports(_ context.Context, req *adminv1.ListISCSIExportsRequest, _ ...grpc.CallOption) (*adminv1.ListISCSIExportsResponse, error) {
	f.snapshotRequest = req
	return &adminv1.ListISCSIExportsResponse{
		RegistryRevision: 8, ConfigGeneration: 3,
		Exports:                  []*adminv1.ISCSIExportSummary{fakeReloadExport()},
		ServingRegistryAuthority: iscsiGatewayRegistryAuthority,
		StorageLayout:            iscsiGatewayRegistryLayout,
	}, nil
}

func fakeReloadExport() *adminv1.ISCSIExportSummary {
	return &adminv1.ISCSIExportSummary{
		ExportId: "export-a", VolumeId: "volume-a",
		TargetIqn: "iqn.2026-08.io.namrbd:export-a", LunWwn: "naa.1",
		Enabled: true, ActiveIscsiGatewayId: "iscsi-gw-a",
		ExportEpoch: 2, ReadWriteAllowed: true,
	}
}
