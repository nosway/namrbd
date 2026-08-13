package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nosway/namrbd/gateway/service"
)

type GatewayLease struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
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

func (r *EtcdRepository) StartGatewayLease(parent context.Context, rec service.GatewayRecord, ttl time.Duration) (*GatewayLease, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	handle := &GatewayLease{cancel: cancel, done: done}

	lease, err := r.client.Grant(ctx, int64(ttl/time.Second))
	if err != nil {
		cancel()
		close(done)
		return nil, err
	}
	if err := r.putGatewayWithLease(ctx, rec, lease.ID); err != nil {
		cancel()
		close(done)
		return nil, err
	}

	kaCh, err := r.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		cancel()
		close(done)
		return nil, err
	}

	go func() {
		defer close(done)
		ticker := time.NewTicker(ttl / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				_, _ = r.client.Delete(context.Background(), r.gatewayStatusKey(rec.GatewayID))
				return
			case _, ok := <-kaCh:
				if !ok {
					return
				}
			case <-ticker.C:
				if err := r.putGatewayWithLease(ctx, rec, lease.ID); err != nil {
					return
				}
			}
		}
	}()

	return handle, nil
}

func (r *EtcdRepository) putGatewayWithLease(ctx context.Context, rec service.GatewayRecord, leaseID clientv3.LeaseID) error {
	if err := r.validateGatewayRecordAgainstRegistry(ctx, rec); err != nil {
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
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = r.client.Put(ctx, r.gatewayStatusKey(rec.GatewayID), string(payload), clientv3.WithLease(leaseID))
	return err
}

func (r *EtcdRepository) gatewayLeaseTTLSeconds(ctx context.Context, leaseID clientv3.LeaseID) int64 {
	resp, err := r.client.TimeToLive(ctx, leaseID)
	if err != nil || resp == nil || resp.TTL <= 0 {
		return 30
	}
	return resp.TTL
}
