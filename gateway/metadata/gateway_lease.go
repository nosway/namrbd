package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nosway/namrbd/gateway/service"
)

type GatewayLease struct {
	cancel  context.CancelFunc
	done    chan struct{}
	updates chan gatewayLeaseUpdate
	once    sync.Once
}

type gatewayLeaseUpdate struct {
	ctx    context.Context
	record service.GatewayRecord
	done   chan error
}

func (l *GatewayLease) Close() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.cancel()
		<-l.done
	})
}

// Update publishes a lifecycle change on the gateway's existing lease. Fleet
// identity cannot change while a process is running, but readiness, drain, and
// error evidence must not wait for the next periodic status refresh.
func (l *GatewayLease) Update(ctx context.Context, rec service.GatewayRecord) error {
	if l == nil {
		return fmt.Errorf("gateway lease is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req := gatewayLeaseUpdate{ctx: ctx, record: rec, done: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.done:
		return fmt.Errorf("gateway lease is closed")
	case l.updates <- req:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.done:
		return fmt.Errorf("gateway lease is closed")
	case err := <-req.done:
		return err
	}
}

const defaultGatewayStatusRefreshInterval = 5 * time.Second

func (r *EtcdRepository) StartGatewayLease(parent context.Context, rec service.GatewayRecord, ttl, refreshInterval time.Duration) (*GatewayLease, error) {
	var err error
	ttl, refreshInterval, err = normalizeGatewayLeaseCadence(ttl, refreshInterval)
	if err != nil {
		return nil, err
	}
	rec = service.NormalizeGatewayFleetRecord(rec)
	if err := service.ValidateGatewayFleetRecord(rec); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	handle := &GatewayLease{cancel: cancel, done: done, updates: make(chan gatewayLeaseUpdate)}

	lease, err := r.client.Grant(ctx, int64(ttl/time.Second))
	r.observeEtcd(err)
	if err != nil {
		cancel()
		close(done)
		return nil, err
	}
	if err := r.putGatewayWithLease(ctx, rec, lease.ID, true); err != nil {
		cancel()
		close(done)
		return nil, err
	}

	kaCh, err := r.client.KeepAlive(ctx, lease.ID)
	r.observeEtcd(err)
	if err != nil {
		cancel()
		close(done)
		return nil, err
	}

	// The lease itself is renewed by etcd's KeepAlive. This timer only refreshes
	// the record contents, so its cadence is about status freshness.
	//
	// The interval is re-jittered on every tick rather than offset once at
	// startup, because a single startup offset keeps a fleet evenly spaced only
	// until one node restarts.
	base := refreshInterval
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(lease.ID)))
	go func() {
		defer close(done)
		timer := time.NewTimer(jitteredInterval(base, rnd))
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				_, _ = r.client.Delete(context.Background(), r.gatewayStatusKey(rec.GatewayID))
				return
			case update := <-handle.updates:
				updated := service.NormalizeGatewayFleetRecord(update.record)
				if err := validateGatewayLeaseUpdate(rec, updated); err != nil {
					update.done <- err
					continue
				}
				err := r.putGatewayWithLease(update.ctx, updated, lease.ID, false)
				r.observeEtcd(err)
				if err == nil {
					rec = updated
				}
				update.done <- err
			case _, ok := <-kaCh:
				if !ok {
					return
				}
			case <-timer.C:
				// Renewal does not re-validate against the fleet prefix.
				err := r.putGatewayWithLease(ctx, rec, lease.ID, false)
				r.observeEtcd(err)
				if err != nil && ctx.Err() != nil {
					return
				}
				// A failed refresh is not fatal. etcd's own KeepAlive holds the
				// lease, so the record survives; only its contents go stale, and
				// the observer above is what makes that visible. Returning here
				// would end status refresh for the life of the process on one
				// transient error, and the gateway would look permanently
				// current at whatever it last wrote.
				timer.Reset(jitteredInterval(base, rnd))
			}
		}
	}()

	return handle, nil
}

func validateGatewayLeaseUpdate(current, updated service.GatewayRecord) error {
	if err := service.ValidateGatewayFleetRecord(updated); err != nil {
		return err
	}
	if current.GatewayID != updated.GatewayID {
		return fmt.Errorf("gateway lease cannot change gateway_id from %q to %q", current.GatewayID, updated.GatewayID)
	}
	if current.Role != updated.Role {
		return fmt.Errorf("gateway lease cannot change role from %q to %q", current.Role, updated.Role)
	}
	return validateGatewayRecordCompatibility(current, updated)
}

func normalizeGatewayLeaseCadence(ttl, refreshInterval time.Duration) (time.Duration, time.Duration, error) {
	if ttl <= 0 {
		ttl = defaultLeaseTTL * time.Second
	}
	if refreshInterval <= 0 {
		refreshInterval = defaultGatewayStatusRefreshInterval
	}
	if refreshInterval >= ttl {
		return 0, 0, fmt.Errorf("gateway status refresh interval %s must be shorter than lease TTL %s", refreshInterval, ttl)
	}
	return ttl, refreshInterval, nil
}

// putGatewayWithLease writes this gateway's own status record.
//
// validate is true only on registration. The fields
// validateGatewayRecordAgainstRegistry checks, cluster and SBS cluster
// identity, metadata backend and roots, all come from startup configuration
// and cannot change while the process runs, so re-checking them on every
// renewal re-reads the whole fleet prefix to learn nothing. The check is also
// symmetric: a gateway joining with a mismatched identity validates against
// this one, so one side performing it is enough.
func (r *EtcdRepository) putGatewayWithLease(ctx context.Context, rec service.GatewayRecord, leaseID clientv3.LeaseID, validate bool) error {
	rec = service.NormalizeGatewayFleetRecord(rec)
	if err := r.validateForLeaseWrite(ctx, rec, validate); err != nil {
		return err
	}
	now := time.Now()
	rec.LastSeenUnix = now.Unix()
	rec.LeaseID = fmt.Sprintf("%d", leaseID)
	leaseExpiry := now.Add(time.Duration(r.gatewayLeaseTTLSeconds(ctx, leaseID)) * time.Second)
	rec.LeaseExpiresAtUnix = leaseExpiry.Unix()
	if rec.StartedAtUnix == 0 {
		rec.StartedAtUnix = rec.LastSeenUnix
	}
	rec.RegistryRevision = 0
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = r.client.Put(ctx, r.gatewayStatusKey(rec.GatewayID), string(payload), clientv3.WithLease(leaseID))
	if err == nil {
		r.pressure.countStatusWrite()
	}
	return err
}

func (r *EtcdRepository) gatewayLeaseTTLSeconds(ctx context.Context, leaseID clientv3.LeaseID) int64 {
	resp, err := r.client.TimeToLive(ctx, leaseID)
	if err != nil || resp == nil || resp.TTL <= 0 {
		return defaultLeaseTTL
	}
	return resp.TTL
}

// validateForLeaseWrite decides whether a status write re-checks the fleet.
//
// It is a named function rather than an inline branch so the skip is
// observable: the counter it increments is what turns "renewals no longer scan
// the fleet prefix" from a claim into a measurement.
func (r *EtcdRepository) validateForLeaseWrite(ctx context.Context, rec service.GatewayRecord, validate bool) error {
	if !validate {
		r.pressure.countSkippedValidation()
		return nil
	}
	return r.validateGatewayRecordAgainstRegistry(ctx, rec)
}
