package service

import (
	"fmt"
	"strings"
)

const GatewayFleetSchemaVersion = 1

// NormalizeGatewayFleetRecord fills fields omitted by records written before
// the Phase AA fleet envelope. It lets a rolling restart read the previous
// shape while every new write publishes the complete v1 contract.
func NormalizeGatewayFleetRecord(rec GatewayRecord) GatewayRecord {
	if rec.SchemaVersion == 0 {
		rec.SchemaVersion = GatewayFleetSchemaVersion
	}
	if rec.Product == "" {
		rec.Product = GatewayProductNAMRBD
	}
	if rec.Role == "" {
		rec.Role = GatewayRoleBlock
	}
	if rec.Readiness == "" {
		switch rec.ConnectionState {
		case GatewayStateUp:
			rec.Readiness = GatewayReadinessReady
		case GatewayStateDegraded:
			rec.Readiness = GatewayReadinessDegraded
		default:
			rec.Readiness = GatewayReadinessBlocked
		}
	}
	if rec.DrainState == "" {
		rec.DrainState = GatewayDrainActive
	}
	return rec
}

// ValidateGatewayFleetRecord validates only fleet-liveness authority. SBS
// placement and iSCSI export mappings deliberately have no representation in
// this envelope.
func ValidateGatewayFleetRecord(rec GatewayRecord) error {
	rec = NormalizeGatewayFleetRecord(rec)
	if rec.SchemaVersion != GatewayFleetSchemaVersion {
		return fmt.Errorf("unsupported gateway fleet schema_version %d", rec.SchemaVersion)
	}
	if strings.TrimSpace(rec.GatewayID) == "" {
		return fmt.Errorf("gateway_id is required")
	}
	switch rec.Product {
	case GatewayProductNAMRBD, GatewayProductNAMROS, GatewayProductISCSI:
	default:
		return fmt.Errorf("unsupported gateway product %q", rec.Product)
	}
	switch rec.Role {
	case GatewayRoleBlock, GatewayRoleObject, GatewayRoleISCSI:
	default:
		return fmt.Errorf("unsupported gateway role %q", rec.Role)
	}
	switch rec.Readiness {
	case GatewayReadinessReady, GatewayReadinessDegraded, GatewayReadinessBlocked:
	default:
		return fmt.Errorf("unsupported gateway readiness %q", rec.Readiness)
	}
	switch rec.DrainState {
	case GatewayDrainActive, GatewayDrainDraining, GatewayDrainDrained:
	default:
		return fmt.Errorf("unsupported gateway drain_state %q", rec.DrainState)
	}
	return nil
}
