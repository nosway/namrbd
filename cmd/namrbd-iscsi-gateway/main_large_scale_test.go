package main

import (
	"context"
	"testing"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/iscsi"
)

func TestLargeScaleRuntimePreparerBindsRegistryFenceToken(t *testing.T) {
	client := service.NewInMemorySBSClient([]service.VolumeSpec{{
		ID: service.HexVolumeID(101), Name: "vol-a", SizeBytes: 4096 * 8, BlockSize: 4096,
	}})
	state := iscsi.RegistryExportState{
		ExportID: "export-a", VolumeID: "00000065", TargetIQN: "iqn.2026-08.io.namrbd:export-a",
		LUNWWN: iscsi.LUNWWN("export-a"), Enabled: true, ActiveGatewayID: "gw-a",
		ExportLeaseID: "lease-a", ExportEpoch: 7, ReadWriteAllowed: true,
		WriteAdmissionState: "read_write",
	}
	runtime, err := (largeScaleSBSRuntimePreparer{
		client: client, gatewayID: "gw-a", adminEndpoint: "sbs-service:9443",
		logicalBlockSize: iscsi.DefaultLogicalBlock,
	}).Prepare(context.Background(), state)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer runtime.Close()
	if runtime.Spec.ExportLeaseID != "lease-a" || runtime.Spec.ExportEpoch != 7 || runtime.Spec.ActiveGatewayID != "gw-a" {
		t.Fatalf("prepared fence token=%+v", runtime.Spec)
	}
	if runtime.Spec.BackingStore == nil || runtime.Spec.SizeBytes != 4096*8 {
		t.Fatalf("prepared runtime=%+v", runtime.Spec)
	}
}

func TestLargeScaleRuntimePreparerRejectsMissingFence(t *testing.T) {
	_, err := (largeScaleSBSRuntimePreparer{
		client: service.NewInMemorySBSClient(nil), gatewayID: "gw-a",
	}).Prepare(context.Background(), iscsi.RegistryExportState{ExportID: "export-a"})
	if err == nil {
		t.Fatal("registry export without lease/epoch was accepted")
	}
}
