package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"google.golang.org/grpc"
)

func TestLoadISCSIGatewayExportSnapshotPinsPagesAndSelectsGateway(t *testing.T) {
	all := make([]*adminv1.ISCSIExportSummary, 0, 1000)
	for i := 0; i < 1000; i++ {
		active := "iscsi-gw-other"
		if i < 32 {
			active = "iscsi-gw-a"
		}
		standby := []string(nil)
		if i >= 32 && i < 64 {
			standby = []string{"iscsi-gw-a"}
		}
		all = append(all, &adminv1.ISCSIExportSummary{
			ExportId:               fmt.Sprintf("export-%04d", i),
			TargetIqn:              fmt.Sprintf("iqn.2026-08.io.namrbd:export-%04d", i),
			LunId:                  uint64(i),
			LunWwn:                 fmt.Sprintf("naa.%016x", i+1),
			VolumeId:               fmt.Sprintf("volume-%04d", i),
			Enabled:                true,
			ActiveIscsiGatewayId:   active,
			StandbyIscsiGatewayIds: standby,
			ExportEpoch:            12,
		})
	}
	client := &pagedExportLister{exports: all}
	snapshot, err := loadISCSIGatewayExportSnapshot(context.Background(), client, "iscsi-gw-a", 64)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.RegistryRevision != 77 || snapshot.ConfigGeneration != 19 || snapshot.PageCount != 8 {
		t.Fatalf("snapshot checkpoint = %#v", snapshot)
	}
	if len(snapshot.Exports) != 64 {
		t.Fatalf("selected exports = %d, want 64", len(snapshot.Exports))
	}
	if len(client.requests) != 8 || client.requests[0].GetRegistryRevision() != 0 {
		t.Fatalf("page requests = %#v", client.requests)
	}
	for i, req := range client.requests[1:] {
		if req.GetRegistryRevision() != 77 {
			t.Fatalf("page %d was not pinned: %#v", i+2, req)
		}
	}
}

func TestLoadISCSIGatewayExportSnapshotRejectsCapWithoutSnapshot(t *testing.T) {
	all := make([]*adminv1.ISCSIExportSummary, 65)
	for i := range all {
		all[i] = &adminv1.ISCSIExportSummary{
			ExportId:             fmt.Sprintf("export-%02d", i),
			Enabled:              true,
			ActiveIscsiGatewayId: "iscsi-gw-a",
		}
	}
	client := &pagedExportLister{exports: all}
	snapshot, err := loadISCSIGatewayExportSnapshot(context.Background(), client, "iscsi-gw-a", 64)
	if err == nil || !strings.Contains(err.Error(), "exceeding max_exports_per_process=64") {
		t.Fatalf("cap error = %v", err)
	}
	if snapshot.RegistryRevision != 0 || len(snapshot.Exports) != 0 {
		t.Fatalf("over-cap generation leaked to caller: %#v", snapshot)
	}
}

type pagedExportLister struct {
	exports  []*adminv1.ISCSIExportSummary
	requests []*adminv1.ListISCSIExportsRequest
}

func (f *pagedExportLister) ListISCSIExports(_ context.Context, req *adminv1.ListISCSIExportsRequest, _ ...grpc.CallOption) (*adminv1.ListISCSIExportsResponse, error) {
	f.requests = append(f.requests, req)
	if req.GetRegistryRevision() != 0 && req.GetRegistryRevision() != 77 {
		return nil, fmt.Errorf("stale revision %d", req.GetRegistryRevision())
	}
	start := 0
	if req.GetPageToken() != "" {
		var err error
		start, err = strconv.Atoi(req.GetPageToken())
		if err != nil {
			return nil, err
		}
	}
	end := start + int(req.GetPageSize())
	if end > len(f.exports) {
		end = len(f.exports)
	}
	next := ""
	if end < len(f.exports) {
		next = strconv.Itoa(end)
	}
	return &adminv1.ListISCSIExportsResponse{
		RegistryRevision:         77,
		ConfigGeneration:         19,
		Exports:                  f.exports[start:end],
		NextPageToken:            next,
		ServingRegistryAuthority: iscsiGatewayRegistryAuthority,
		StorageLayout:            iscsiGatewayRegistryLayout,
	}, nil
}
