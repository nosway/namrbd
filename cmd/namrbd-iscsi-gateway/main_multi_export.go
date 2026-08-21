package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/nosway/namrbd/iscsi"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"google.golang.org/grpc"
)

const (
	iscsiGatewayRegistryPageSize  = 128
	iscsiGatewayRegistryAuthority = "sbs_service_tikv"
	iscsiGatewayRegistryLayout    = "split_v2"
)

type iscsiGatewayExportLister interface {
	ListISCSIExports(context.Context, *adminv1.ListISCSIExportsRequest, ...grpc.CallOption) (*adminv1.ListISCSIExportsResponse, error)
}

type iscsiGatewayExportSnapshot struct {
	RegistryRevision uint64
	ConfigGeneration uint64
	Exports          []*adminv1.ISCSIExportSummary
	PageCount        int
}

// loadISCSIGatewayExportSnapshot selects this gateway's complete immutable
// serving generation from revision-pinned TiKV pages. It never accepts a
// command-line target/LUN selector as mapping authority.
func loadISCSIGatewayExportSnapshot(ctx context.Context, client iscsiGatewayExportLister, gatewayID string, maxExports int) (iscsiGatewayExportSnapshot, error) {
	if client == nil {
		return iscsiGatewayExportSnapshot{}, fmt.Errorf("iSCSI registry client is required")
	}
	gatewayID = strings.TrimSpace(gatewayID)
	if gatewayID == "" {
		return iscsiGatewayExportSnapshot{}, fmt.Errorf("iSCSI gateway id is required for registry selection")
	}
	if maxExports == 0 {
		maxExports = iscsi.DefaultMaxExportsPerProcess
	}
	if maxExports < 1 || maxExports > iscsi.DefaultMaxExportsPerProcess {
		return iscsiGatewayExportSnapshot{}, fmt.Errorf("max_exports_per_process must be 1..%d, got %d", iscsi.DefaultMaxExportsPerProcess, maxExports)
	}

	snapshot := iscsiGatewayExportSnapshot{}
	seen := make(map[string]struct{}, maxExports)
	pageToken := ""
	for {
		resp, err := client.ListISCSIExports(ctx, &adminv1.ListISCSIExportsRequest{
			PageSize:         iscsiGatewayRegistryPageSize,
			PageToken:        pageToken,
			RegistryRevision: snapshot.RegistryRevision,
		})
		if err != nil {
			return iscsiGatewayExportSnapshot{}, fmt.Errorf("list iSCSI export page %d: %w", snapshot.PageCount+1, err)
		}
		if resp.GetServingRegistryAuthority() != iscsiGatewayRegistryAuthority {
			return iscsiGatewayExportSnapshot{}, fmt.Errorf("iSCSI serving registry authority is %q, want %q", resp.GetServingRegistryAuthority(), iscsiGatewayRegistryAuthority)
		}
		if resp.GetStorageLayout() != iscsiGatewayRegistryLayout {
			return iscsiGatewayExportSnapshot{}, fmt.Errorf("iSCSI registry storage layout is %q, want %q", resp.GetStorageLayout(), iscsiGatewayRegistryLayout)
		}
		if snapshot.PageCount == 0 {
			snapshot.RegistryRevision = resp.GetRegistryRevision()
			snapshot.ConfigGeneration = resp.GetConfigGeneration()
		} else if resp.GetRegistryRevision() != snapshot.RegistryRevision || resp.GetConfigGeneration() != snapshot.ConfigGeneration {
			return iscsiGatewayExportSnapshot{}, fmt.Errorf("iSCSI registry page changed generation: revision/config %d/%d, want %d/%d",
				resp.GetRegistryRevision(), resp.GetConfigGeneration(), snapshot.RegistryRevision, snapshot.ConfigGeneration)
		}
		snapshot.PageCount++
		for _, export := range resp.GetExports() {
			if !export.GetEnabled() || !exportAssignedToGateway(export, gatewayID) {
				continue
			}
			exportID := strings.TrimSpace(export.GetExportId())
			if exportID == "" {
				return iscsiGatewayExportSnapshot{}, fmt.Errorf("registry revision %d contains an assigned export without export_id", snapshot.RegistryRevision)
			}
			if _, exists := seen[exportID]; exists {
				return iscsiGatewayExportSnapshot{}, fmt.Errorf("registry revision %d contains duplicate export_id %q", snapshot.RegistryRevision, exportID)
			}
			seen[exportID] = struct{}{}
			snapshot.Exports = append(snapshot.Exports, export)
			if len(snapshot.Exports) > maxExports {
				return iscsiGatewayExportSnapshot{}, fmt.Errorf("registry selected %d exports for gateway %q, exceeding max_exports_per_process=%d", len(snapshot.Exports), gatewayID, maxExports)
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return snapshot, nil
}

func exportAssignedToGateway(export *adminv1.ISCSIExportSummary, gatewayID string) bool {
	if export == nil {
		return false
	}
	if strings.TrimSpace(export.GetActiveIscsiGatewayId()) == gatewayID {
		return true
	}
	for _, standby := range export.GetStandbyIscsiGatewayIds() {
		if strings.TrimSpace(standby) == gatewayID {
			return true
		}
	}
	return false
}
