package metadata

import (
	"testing"

	"github.com/nosway/namrbd/gateway/service"
)

func TestValidateGatewayRecordCompatibilityAllowsMatchingIdentity(t *testing.T) {
	existing := service.GatewayRecord{
		GatewayID:                 "gw-a",
		ClusterID:                 "namrbd:etcd:/namrbd-prod",
		SBSClusterID:              "sbs:pebble:sbs/cluster/prod",
		MetadataBackend:           "etcd",
		MetadataRoot:              "/namrbd-prod",
		SBSClusterMetadataBackend: "pebble",
		SBSClusterMetadataRoot:    "sbs/cluster/prod",
	}
	incoming := existing
	incoming.GatewayID = "gw-b"

	if err := validateGatewayRecordCompatibility(existing, incoming); err != nil {
		t.Fatalf("validateGatewayRecordCompatibility: %v", err)
	}
}

func TestValidateGatewayRecordCompatibilityRejectsClusterMismatch(t *testing.T) {
	existing := service.GatewayRecord{
		GatewayID:       "gw-a",
		ClusterID:       "namrbd:etcd:/namrbd-prod",
		MetadataBackend: "etcd",
		MetadataRoot:    "/namrbd-prod",
	}
	incoming := service.GatewayRecord{
		GatewayID:       "gw-b",
		ClusterID:       "namrbd:etcd:/namrbd-dev",
		MetadataBackend: "etcd",
		MetadataRoot:    "/namrbd-dev",
	}

	if err := validateGatewayRecordCompatibility(existing, incoming); err == nil {
		t.Fatalf("expected mismatch error")
	}
}

func TestValidateGatewayRecordCompatibilityRejectsSBSMetadataMismatch(t *testing.T) {
	existing := service.GatewayRecord{
		GatewayID:                 "gw-a",
		SBSClusterID:              "sbs:pebble:sbs/cluster/prod",
		SBSClusterMetadataBackend: "pebble",
		SBSClusterMetadataRoot:    "sbs/cluster/prod",
	}
	incoming := service.GatewayRecord{
		GatewayID:                 "gw-b",
		SBSClusterID:              "sbs:pebble:sbs/cluster/dev",
		SBSClusterMetadataBackend: "pebble",
		SBSClusterMetadataRoot:    "sbs/cluster/dev",
	}

	if err := validateGatewayRecordCompatibility(existing, incoming); err == nil {
		t.Fatalf("expected mismatch error")
	}
}
