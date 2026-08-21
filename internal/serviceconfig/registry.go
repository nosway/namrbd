package serviceconfig

import (
	"fmt"
	"strings"

	"github.com/nosway/namrbd/internal/envcompat"
)

// RegistryFor returns the fields a process permits outside its config file.
//
// The list is short on purpose. These are the settings that genuinely differ
// per host: this node's identity, the addresses it binds, and the addresses it
// advertises. Everything else, meaning every setting that should be identical
// across the fleet, is only settable in the reviewed config file. A tunable
// that can be supplied per host will eventually differ per host.
func RegistryFor(process string) []Overridable {
	switch process {
	case ProcessGateway:
		return gatewayOverrides()
	case ProcessISCSIGateway:
		return iscsiGatewayOverrides()
	case ProcessSBSService:
		return sbsServiceOverrides()
	case ProcessSBSData:
		return sbsDataOverrides()
	case ProcessCSIDriver:
		return csiDriverOverrides()
	case ProcessMCP:
		return mcpOverrides()
	default:
		return nil
	}
}

// AllRegistries is used by tests and tooling that need every entry at once.
func AllRegistries() map[string][]Overridable {
	out := map[string][]Overridable{}
	for _, p := range sortedProcesses() {
		out[p] = RegistryFor(p)
	}
	return out
}

func requireGateway(f *File) (*GatewayConfig, error) {
	if f.Gateway == nil {
		return nil, fmt.Errorf("config has no gateway block")
	}
	return f.Gateway, nil
}

func gatewayOverrides() []Overridable {
	str := func(field, env, flag string, pick func(*GatewayConfig) *string) Overridable {
		return Overridable{Field: field, Env: env, Flag: flag, apply: func(f *File, v string) error {
			g, err := requireGateway(f)
			if err != nil {
				return err
			}
			*pick(g) = v
			return nil
		}}
	}
	compatStr := func(field string, spec envcompat.Spec, flag string, pick func(*GatewayConfig) *string) Overridable {
		o := str(field, spec.Canonical, flag, pick)
		o.LegacyEnvs = append([]envcompat.Legacy(nil), spec.Legacy...)
		return o
	}
	return []Overridable{
		str("gateway.gateway_id", "NAMRBD_GATEWAY_ID", "gateway-id",
			func(g *GatewayConfig) *string { return &g.GatewayID }),
		compatStr("gateway.listen", envcompat.GatewayControlListen, "control-http-listen",
			func(g *GatewayConfig) *string { return &g.Listen }),
		str("gateway.data_listen", "NAMRBD_GATEWAY_DATA_LISTEN", "data-listen",
			func(g *GatewayConfig) *string { return &g.DataListen }),
		str("gateway.advertise_control_address", "NAMRBD_GATEWAY_ADVERTISE_CONTROL_ADDRESS", "advertise-control-address",
			func(g *GatewayConfig) *string { return &g.AdvertiseControlAddr }),
		str("gateway.advertise_data_address", "NAMRBD_GATEWAY_ADVERTISE_DATA_ADDRESS", "advertise-data-address",
			func(g *GatewayConfig) *string { return &g.AdvertiseDataAddr }),
		compatStr("gateway.sbs_admin_endpoint", envcompat.GatewaySBSServiceEndpoint, "sbs-service-endpoint",
			func(g *GatewayConfig) *string { return &g.SBSAdminEndpoint }),
	}
}

func iscsiGatewayOverrides() []Overridable {
	str := func(field, env, flag string, pick func(*ISCSIGatewayConfig) *string) Overridable {
		return Overridable{Field: field, Env: env, Flag: flag, apply: func(f *File, v string) error {
			if f.ISCSIGetway == nil {
				return fmt.Errorf("config has no iscsi_gateway block")
			}
			*pick(f.ISCSIGetway) = v
			return nil
		}}
	}
	compatStr := func(field string, spec envcompat.Spec, flag string, pick func(*ISCSIGatewayConfig) *string) Overridable {
		o := str(field, spec.Canonical, flag, pick)
		o.LegacyEnvs = append([]envcompat.Legacy(nil), spec.Legacy...)
		return o
	}
	return []Overridable{
		str("iscsi_gateway.gateway_id", "NAMRBD_ISCSI_GATEWAY_ID", "iscsi-gateway-id",
			func(g *ISCSIGatewayConfig) *string { return &g.GatewayID }),
		compatStr("iscsi_gateway.sbs_endpoint", envcompat.ISCSISBSDataEndpoint, "sbs-data-endpoint",
			func(g *ISCSIGatewayConfig) *string { return &g.SBSEndpoint }),
		compatStr("iscsi_gateway.sbs_admin_endpoint", envcompat.ISCSISBSServiceEndpoint, "sbs-service-endpoint",
			func(g *ISCSIGatewayConfig) *string { return &g.SBSAdminEndpoint }),
		{Field: "iscsi_gateway.advertise_portals", Env: "NAMRBD_ISCSI_ADVERTISE_PORTALS", Flag: "advertise-portals",
			apply: func(f *File, v string) error {
				if f.ISCSIGetway == nil {
					return fmt.Errorf("config has no iscsi_gateway block")
				}
				parts := []string{}
				for _, p := range strings.Split(v, ",") {
					if p = strings.TrimSpace(p); p != "" {
						parts = append(parts, p)
					}
				}
				if len(parts) == 0 {
					return fmt.Errorf("advertise_portals override is empty")
				}
				f.ISCSIGetway.AdvertisePortals = parts
				return nil
			}},
	}
}

func sbsServiceOverrides() []Overridable {
	str := func(field, env, flag string, pick func(*SBSServiceConfig) *string) Overridable {
		return Overridable{Field: field, Env: env, Flag: flag, apply: func(f *File, v string) error {
			if f.SBSService == nil {
				return fmt.Errorf("config has no sbs_service block")
			}
			*pick(f.SBSService) = v
			return nil
		}}
	}
	compatStr := func(field string, spec envcompat.Spec, flag string, pick func(*SBSServiceConfig) *string) Overridable {
		o := str(field, spec.Canonical, flag, pick)
		o.LegacyEnvs = append([]envcompat.Legacy(nil), spec.Legacy...)
		return o
	}
	return []Overridable{
		compatStr("sbs_service.node_id", envcompat.SBSServiceNodeID, "node-id",
			func(s *SBSServiceConfig) *string { return &s.NodeID }),
		compatStr("sbs_service.grpc_listen", envcompat.SBSServiceGRPCListen, "sbs-service-listen",
			func(s *SBSServiceConfig) *string { return &s.GRPCListen }),
		compatStr("sbs_service.http_listen", envcompat.SBSServiceHTTPListen, "sbs-service-http-listen",
			func(s *SBSServiceConfig) *string { return &s.HTTPListen }),
	}
}

func sbsDataOverrides() []Overridable {
	str := func(field, env, flag string, pick func(*SBSDataConfig) *string) Overridable {
		return Overridable{Field: field, Env: env, Flag: flag, apply: func(f *File, v string) error {
			if f.SBSData == nil {
				return fmt.Errorf("config has no sbs_data block")
			}
			*pick(f.SBSData) = v
			return nil
		}}
	}
	compatStr := func(field string, spec envcompat.Spec, flag string, pick func(*SBSDataConfig) *string) Overridable {
		o := str(field, spec.Canonical, flag, pick)
		o.LegacyEnvs = append([]envcompat.Legacy(nil), spec.Legacy...)
		return o
	}
	return []Overridable{
		str("sbs_data.node_id", "NAMRBD_SBS_DATA_NODE_ID", "node-id",
			func(d *SBSDataConfig) *string { return &d.NodeID }),
		compatStr("sbs_data.data_path", envcompat.SBSDataPath, "path",
			func(d *SBSDataConfig) *string { return &d.DataPath }),
		compatStr("sbs_data.grpc_listen", envcompat.SBSDataGRPCListen, "sbs-data-listen",
			func(d *SBSDataConfig) *string { return &d.GRPCListen }),
		compatStr("sbs_data.http_listen", envcompat.SBSDataHTTPListen, "sbs-data-http-listen",
			func(d *SBSDataConfig) *string { return &d.HTTPListen }),
	}
}

func csiDriverOverrides() []Overridable {
	str := func(field, env, flag string, pick func(*CSIDriverConfig) *string) Overridable {
		return Overridable{Field: field, Env: env, Flag: flag, apply: func(f *File, v string) error {
			if f.CSIDriver == nil {
				return fmt.Errorf("config has no csi_driver block")
			}
			*pick(f.CSIDriver) = v
			return nil
		}}
	}
	compatStr := func(field string, spec envcompat.Spec, flag string, apply func(*CSIDriverConfig, string) error) Overridable {
		return Overridable{
			Field: field, Env: spec.Canonical, LegacyEnvs: append([]envcompat.Legacy(nil), spec.Legacy...), Flag: flag,
			apply: func(f *File, v string) error {
				if f.CSIDriver == nil {
					return fmt.Errorf("config has no csi_driver block")
				}
				return apply(f.CSIDriver, v)
			},
		}
	}
	return []Overridable{
		str("csi_driver.node_id", "NAMRBD_CSI_NODE_ID", "node-id",
			func(c *CSIDriverConfig) *string { return &c.NodeID }),
		str("csi_driver.endpoint", "NAMRBD_CSI_ENDPOINT", "endpoint",
			func(c *CSIDriverConfig) *string { return &c.Endpoint }),
		compatStr("csi_driver.admin_endpoints", envcompat.CSISBSServiceEndpoints, "admin-endpoints",
			func(c *CSIDriverConfig, value string) error {
				endpoints := strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' })
				if len(endpoints) == 0 {
					return fmt.Errorf("SBS service endpoint list is empty")
				}
				c.AdminEndpoints = endpoints
				return nil
			}),
		compatStr("csi_driver.admin_endpoints.0", envcompat.CSISBSServiceEndpoint, "admin-endpoint",
			func(c *CSIDriverConfig, value string) error {
				if len(c.AdminEndpoints) == 0 {
					c.AdminEndpoints = []string{value}
				} else {
					c.AdminEndpoints[0] = value
				}
				return nil
			}),
	}
}

func mcpOverrides() []Overridable {
	return []Overridable{
		{Field: "mcp.operations_endpoint", Env: "NAMRBD_MCP_OPERATIONS_ENDPOINT", Flag: "operations-endpoint",
			apply: func(f *File, v string) error {
				if f.MCP == nil {
					return fmt.Errorf("config has no mcp block")
				}
				f.MCP.OperationsEndpoint = v
				return nil
			}},
	}
}
