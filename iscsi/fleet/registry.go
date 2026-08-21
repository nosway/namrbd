package fleet

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/gateway/metadata"
	"github.com/nosway/namrbd/gateway/service"
)

const (
	MembershipAuthority = "etcd"
	HealthAuthority     = "etcd"
	DefaultEtcdRoot     = "/namrbd/iscsi"

	DefaultLeaseTTL        = 15 * time.Second
	DefaultStatusRefresh   = 5 * time.Second
	DefaultEtcdDialTimeout = 5 * time.Second
)

// Config contains only process-fleet identity. Target, LUN, export, volume,
// lease, and epoch authority deliberately remain absent and TiKV-backed.
type Config struct {
	GatewayID        string
	AdvertisePortals []string
	EtcdEndpoints    []string
	EtcdRoot         string
	BuildVersion     string
	DialTimeout      time.Duration
	LeaseTTL         time.Duration
	RefreshInterval  time.Duration
}

type Summary struct {
	MembershipEtcdReady bool   `json:"iscsi_gateway_membership_etcd_ready"`
	MembershipAuthority string `json:"iscsi_gateway_membership_authority"`
	HealthAuthority     string `json:"iscsi_gateway_health_authority"`
	GatewayCount        int    `json:"iscsi_gateway_count"`
	ReadyCount          int    `json:"iscsi_gateway_ready_count"`
	StaleCount          int    `json:"iscsi_gateway_stale_count"`
}

// Registry owns this process's etcd lease and exposes the bounded fleet
// list/watch contract to operations and failover controllers.
type Registry struct {
	client *metadataClient
	repo   *metadata.EtcdRepository
	lease  *metadata.GatewayLease

	mu     sync.Mutex
	update sync.Mutex
	record service.GatewayRecord
}

// metadataClient keeps closure of the concrete etcd client local without
// exposing it to callers.
type metadataClient struct {
	close func() error
}

func Start(parent context.Context, cfg Config, observe func(error)) (*Registry, error) {
	record, err := RecordFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	if len(cfg.EtcdEndpoints) == 0 {
		return nil, fmt.Errorf("iSCSI gateway etcd endpoints are required")
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = DefaultEtcdDialTimeout
	}
	client, err := metadata.NewEtcdClient(cfg.EtcdEndpoints, dialTimeout)
	if err != nil {
		return nil, err
	}
	etcdRoot := strings.TrimSpace(cfg.EtcdRoot)
	if etcdRoot == "" {
		etcdRoot = DefaultEtcdRoot
	}
	repo := metadata.NewEtcdRepository(client, etcdRoot)
	repo.SetEtcdOutcomeObserver(observe)
	ttl := cfg.LeaseTTL
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	refresh := cfg.RefreshInterval
	if refresh <= 0 {
		refresh = DefaultStatusRefresh
	}
	lease, err := repo.StartGatewayLease(parent, record, ttl, refresh)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return &Registry{
		client: &metadataClient{close: client.Close},
		repo:   repo,
		lease:  lease,
		record: record,
	}, nil
}

func RecordFromConfig(cfg Config) (service.GatewayRecord, error) {
	gatewayID := strings.TrimSpace(cfg.GatewayID)
	if gatewayID == "" {
		return service.GatewayRecord{}, fmt.Errorf("iSCSI gateway id is required for fleet membership")
	}
	portals, endpoints, err := normalizePortals(cfg.AdvertisePortals)
	if err != nil {
		return service.GatewayRecord{}, err
	}
	rec := service.GatewayRecord{
		SchemaVersion:       service.GatewayFleetSchemaVersion,
		GatewayID:           gatewayID,
		Product:             service.GatewayProductISCSI,
		Role:                service.GatewayRoleISCSI,
		ConnectionState:     service.GatewayStateUp,
		Readiness:           service.GatewayReadinessReady,
		DrainState:          service.GatewayDrainActive,
		BuildVersion:        strings.TrimSpace(cfg.BuildVersion),
		AdvertisedAddresses: portals,
		Capabilities:        []string{"iscsi", "multi-portal"},
		DataplaneEndpoints:  endpoints,
	}
	if err := service.ValidateGatewayFleetRecord(rec); err != nil {
		return service.GatewayRecord{}, err
	}
	return rec, nil
}

func normalizePortals(values []string) ([]string, []service.EndpointSpec, error) {
	seen := map[string]bool{}
	portals := make([]string, 0, len(values))
	endpoints := make([]service.EndpointSpec, 0, len(values))
	for _, raw := range values {
		portal := strings.TrimSpace(raw)
		if portal == "" || seen[portal] {
			continue
		}
		host, portText, err := net.SplitHostPort(portal)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid iSCSI advertised portal %q: %w", portal, err)
		}
		port, err := strconv.ParseUint(portText, 10, 16)
		if err != nil || port == 0 {
			return nil, nil, fmt.Errorf("invalid iSCSI advertised portal port in %q", portal)
		}
		if strings.TrimSpace(host) == "" {
			return nil, nil, fmt.Errorf("invalid iSCSI advertised portal host in %q", portal)
		}
		seen[portal] = true
		portals = append(portals, portal)
		endpoints = append(endpoints, service.EndpointSpec{Address: host, Port: uint16(port), AuthMode: "iscsi"})
	}
	if len(portals) == 0 {
		return nil, nil, fmt.Errorf("at least one iSCSI advertised portal is required for fleet membership")
	}
	return portals, endpoints, nil
}

func (r *Registry) Record() service.GatewayRecord {
	if r == nil {
		return service.GatewayRecord{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.record
}

func (r *Registry) SetLifecycle(ctx context.Context, readiness service.GatewayReadiness, drain service.GatewayDrainState, cause error) error {
	if r == nil || r.lease == nil {
		return fmt.Errorf("iSCSI gateway fleet registry is not running")
	}
	r.update.Lock()
	defer r.update.Unlock()
	r.mu.Lock()
	next := r.record
	next.Readiness = readiness
	next.DrainState = drain
	switch readiness {
	case service.GatewayReadinessReady:
		next.ConnectionState = service.GatewayStateUp
	case service.GatewayReadinessDegraded:
		next.ConnectionState = service.GatewayStateDegraded
	default:
		next.ConnectionState = service.GatewayStateDown
	}
	if cause != nil {
		msg := strings.TrimSpace(cause.Error())
		if next.FirstError == "" {
			next.FirstError = msg
		}
		next.LastError = msg
	}
	r.mu.Unlock()
	if err := r.lease.Update(ctx, next); err != nil {
		return err
	}
	r.mu.Lock()
	r.record = next
	r.mu.Unlock()
	return nil
}

func (r *Registry) ListPage(ctx context.Context, opts metadata.GatewayFleetListOptions) (metadata.GatewayFleetPage, Summary, error) {
	if r == nil || r.repo == nil {
		return metadata.GatewayFleetPage{}, Summary{}, fmt.Errorf("iSCSI gateway fleet registry is not running")
	}
	page, err := r.repo.ListGatewayFleetPage(ctx, opts)
	if err != nil {
		return metadata.GatewayFleetPage{}, Summary{}, err
	}
	if err := validateISCSIRecords(page.Records); err != nil {
		return metadata.GatewayFleetPage{}, Summary{}, err
	}
	return page, Summarize(page.Records, time.Now()), nil
}

func (r *Registry) Watch(ctx context.Context, afterRevision int64) (<-chan metadata.GatewayFleetEvent, error) {
	if r == nil || r.repo == nil {
		return nil, fmt.Errorf("iSCSI gateway fleet registry is not running")
	}
	source, err := r.repo.WatchGatewayFleet(ctx, afterRevision)
	if err != nil {
		return nil, err
	}
	out := make(chan metadata.GatewayFleetEvent, 16)
	go func() {
		defer close(out)
		for event := range source {
			if event.Record != nil {
				if err := validateISCSIRecords([]service.GatewayRecord{*event.Record}); err != nil {
					event = metadata.GatewayFleetEvent{
						Type: metadata.GatewayFleetEventResyncRequired, Revision: event.Revision, Reason: err.Error(),
					}
				}
			}
			select {
			case <-ctx.Done():
				return
			case out <- event:
			}
			if event.Type == metadata.GatewayFleetEventResyncRequired {
				return
			}
		}
	}()
	return out, nil
}

func Summarize(records []service.GatewayRecord, now time.Time) Summary {
	summary := Summary{
		MembershipEtcdReady: true,
		MembershipAuthority: MembershipAuthority,
		HealthAuthority:     HealthAuthority,
	}
	for _, rec := range records {
		rec = service.NormalizeGatewayFleetRecord(rec)
		if rec.Product != service.GatewayProductISCSI || rec.Role != service.GatewayRoleISCSI {
			continue
		}
		summary.GatewayCount++
		stale := rec.LeaseExpiresAtUnix <= 0 || rec.LeaseExpiresAtUnix <= now.Unix()
		if stale {
			summary.StaleCount++
			continue
		}
		if rec.ConnectionState == service.GatewayStateUp &&
			rec.Readiness == service.GatewayReadinessReady &&
			rec.DrainState == service.GatewayDrainActive {
			summary.ReadyCount++
		}
	}
	return summary
}

func validateISCSIRecords(records []service.GatewayRecord) error {
	for _, rec := range records {
		rec = service.NormalizeGatewayFleetRecord(rec)
		if rec.Product != service.GatewayProductISCSI || rec.Role != service.GatewayRoleISCSI {
			return fmt.Errorf("gateway %q is %s/%s inside the iSCSI fleet root", rec.GatewayID, rec.Product, rec.Role)
		}
	}
	return nil
}

func (r *Registry) Close() {
	if r == nil {
		return
	}
	if r.lease != nil {
		r.lease.Close()
	}
	if r.client != nil && r.client.close != nil {
		_ = r.client.close()
	}
}

// SortedGatewayIDs gives controllers deterministic ordering across pages.
func SortedGatewayIDs(records []service.GatewayRecord) []string {
	ids := make([]string, 0, len(records))
	for _, rec := range records {
		if rec.Product == service.GatewayProductISCSI && rec.Role == service.GatewayRoleISCSI {
			ids = append(ids, rec.GatewayID)
		}
	}
	sort.Strings(ids)
	return ids
}
