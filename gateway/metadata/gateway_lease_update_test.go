package metadata

import (
	"testing"

	"github.com/nosway/namrbd/gateway/service"
)

func TestGatewayLeaseUpdateAllowsLifecycleButPreservesIdentity(t *testing.T) {
	current := service.GatewayRecord{
		GatewayID: "iscsi-gw-01", Product: service.GatewayProductISCSI, Role: service.GatewayRoleISCSI,
		ConnectionState: service.GatewayStateUp, Readiness: service.GatewayReadinessReady,
		DrainState: service.GatewayDrainActive,
	}
	updated := current
	updated.ConnectionState = service.GatewayStateDegraded
	updated.Readiness = service.GatewayReadinessDegraded
	updated.DrainState = service.GatewayDrainDraining
	updated.FirstError = "target listener stopped"
	updated.LastError = updated.FirstError
	if err := validateGatewayLeaseUpdate(current, updated); err != nil {
		t.Fatalf("lifecycle update rejected: %v", err)
	}

	updated.GatewayID = "iscsi-gw-02"
	if err := validateGatewayLeaseUpdate(current, updated); err == nil {
		t.Fatal("lease update changed gateway identity")
	}
	updated = current
	updated.Role = service.GatewayRoleBlock
	if err := validateGatewayLeaseUpdate(current, updated); err == nil {
		t.Fatal("lease update changed gateway role")
	}
}
