package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/serviceconfig"
)

// buildConfigFromFlags is the inverse of applyGatewayConfig: it turns a running
// invocation's flag values into a config file.
//
// It exists so adopting config files does not mean hand-rewriting every
// existing deployment. Whatever this cannot carry is reported rather than
// dropped, because a migration that loses a setting quietly is worse than one
// that refuses to finish.
func buildConfigFromFlags(b gatewayConfigBinding, cliSet map[string]string) (*serviceconfig.File, []string, []string) {
	var secrets, dropped []string

	deref := func(p *string) string {
		if p == nil {
			return ""
		}
		return *p
	}
	derefBool := func(p *bool) bool { return p != nil && *p }
	seconds := func(p *time.Duration) int {
		if p == nil {
			return 0
		}
		return int((*p).Seconds())
	}
	derefUint := func(p *uint) int {
		if p == nil {
			return 0
		}
		return int(*p)
	}

	g := &serviceconfig.GatewayConfig{
		GatewayID:            deref(b.GatewayID),
		Listen:               deref(b.ListenAddr),
		DataListen:           deref(b.DataListenAddr),
		AdvertiseControlAddr: deref(b.AdvertiseControlAddr),
		AdvertiseDataAddr:    deref(b.AdvertiseDataAddr),
		DataDisable:          derefBool(b.DataDisable),
		SBSAdminEndpoint:     deref(b.SBSAdminEndpoint),
		MetadataBackend:      deref(b.MetadataBackend),
		DataBackendMode:      deref(b.DataBackendMode),
	}

	if derefBool(b.TLSEnable) || deref(b.TLSCertFile) != "" {
		g.TLS = &serviceconfig.TLSConfig{
			Enable:     derefBool(b.TLSEnable),
			CertFile:   deref(b.TLSCertFile),
			ServerName: deref(b.TLSServerName),
		}
		// The key flag holds a path, so it converts to a file reference
		// directly.
		if v := strings.TrimSpace(deref(b.TLSKeyFile)); v != "" {
			g.TLS.Key = serviceconfig.SecretRef{File: v}
		}
	}
	if v := strings.TrimSpace(deref(b.EtcdEndpoints)); v != "" {
		g.Etcd = &serviceconfig.EtcdConfig{
			Endpoints: splitList(v),
			Root:      deref(b.EtcdRoot),
		}
	}

	g.Cache = serviceconfig.GatewayCacheConfig{
		VolumeTTLSeconds:           seconds(b.VolumeCacheTTL),
		ZeroEvidenceTTLSeconds:     seconds(b.ZeroEvidenceCacheTTL),
		OpenReuseTTLSeconds:        seconds(b.OpenReuseTTL),
		ChunkIDAllocationCacheSize: derefUint(b.ChunkIDAllocationCacheSize),
		WritePlanTTLSeconds:        seconds(b.WritePlanCacheTTL),
		BeginWriteVolumeTTLSeconds: seconds(b.BeginWriteVolumeStateTTL),
	}
	g.Reconcile = serviceconfig.GatewayReconcileConfig{
		PathPlanIntervalSeconds:      seconds(b.PathPlanReconcileInterval),
		LeaseTTLSeconds:              seconds(b.GatewayLeaseTTL),
		StatusRefreshIntervalSeconds: seconds(b.GatewayStatusRefreshInterval),
		ChunkGCIntervalSeconds:       seconds(b.ChunkGCInterval),
	}
	if b.ChunkGCBatchSize != nil {
		g.Reconcile.ChunkGCBatchSize = *b.ChunkGCBatchSize
	}
	g.Dataplane = serviceconfig.GatewayDataplaneConfig{
		MaxInflightRequests: derefUint(b.MaxInflightRequests),
		MaxIOSize:           derefUint(b.MaxIOSize),
		TokenTTLSeconds:     seconds(b.DataplaneTokenTTL),
	}
	if b.MaxInflightBytes != nil {
		g.Dataplane.MaxInflightBytes = int64(*b.MaxInflightBytes)
	}
	if b.DataplaneWireVersion != nil {
		g.Dataplane.WireVersion = *b.DataplaneWireVersion
	}
	// These flags hold the key material itself, not a path to it. There is
	// nowhere safe for that in a config file, so the generated file gets a
	// placeholder and the operator is told which fields to supply.
	g.Dataplane.TokenKey = serviceconfig.SecretRefFor(
		"gateway.dataplane.token_key", deref(b.DataplaneTokenKey), false, &secrets)
	g.Dataplane.SessionKey = serviceconfig.SecretRefFor(
		"gateway.dataplane.session_key", deref(b.DataplaneSessionKey), false, &secrets)

	g.Observability = serviceconfig.ObservabilityConfig{Trace: derefBool(b.DataplaneRequestTrace)}

	// A flag the operator set that has no config key would otherwise vanish.
	for name := range cliSet {
		d, known := serviceconfig.DeprecationFor(serviceconfig.ProcessGateway, name)
		if known && d.ConfigKey == "" {
			dropped = append(dropped, fmt.Sprintf("--%s: %s", name, d.Note))
		}
	}

	return &serviceconfig.File{
		SchemaVersion: serviceconfig.SchemaVersion,
		Revision:      1,
		// A generated file has not been reviewed, and the strict profile refuses
		// settings a flag-started deployment may still rely on.
		Profile: serviceconfig.ProfileDev,
		Process: serviceconfig.ProcessGateway,
		Gateway: g,
	}, secrets, dropped
}

func splitList(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
