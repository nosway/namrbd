package service

import "testing"

func TestNormalizeLegacyGatewayFleetRecord(t *testing.T) {
	rec := NormalizeGatewayFleetRecord(GatewayRecord{
		GatewayID: "gw-a", ConnectionState: GatewayStateDegraded,
	})
	if rec.SchemaVersion != GatewayFleetSchemaVersion || rec.Product != GatewayProductNAMRBD || rec.Role != GatewayRoleBlock {
		t.Fatalf("legacy identity normalization = %+v", rec)
	}
	if rec.Readiness != GatewayReadinessDegraded || rec.DrainState != GatewayDrainActive {
		t.Fatalf("legacy health normalization = %+v", rec)
	}
	if err := ValidateGatewayFleetRecord(rec); err != nil {
		t.Fatalf("normalized legacy record is invalid: %v", err)
	}
}

func TestGatewayFleetEnvelopeSupportsProductRolesAndHealth(t *testing.T) {
	for _, tc := range []GatewayRecord{
		{GatewayID: "block-a", Product: GatewayProductNAMRBD, Role: GatewayRoleBlock, Readiness: GatewayReadinessReady, DrainState: GatewayDrainActive},
		{GatewayID: "object-a", Product: GatewayProductNAMROS, Role: GatewayRoleObject, Readiness: GatewayReadinessDegraded, DrainState: GatewayDrainDraining},
		{GatewayID: "iscsi-a", Product: GatewayProductISCSI, Role: GatewayRoleISCSI, Readiness: GatewayReadinessBlocked, DrainState: GatewayDrainDrained},
	} {
		if err := ValidateGatewayFleetRecord(tc); err != nil {
			t.Errorf("record %+v is invalid: %v", tc, err)
		}
	}
}

func TestGatewayFleetEnvelopeRejectsUnknownAuthorityShape(t *testing.T) {
	rec := GatewayRecord{GatewayID: "gw-a", Product: "sbs-membership", Role: GatewayRoleBlock}
	if err := ValidateGatewayFleetRecord(rec); err == nil {
		t.Fatal("an unknown product was accepted as fleet membership")
	}
}
