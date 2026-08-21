package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nosway/namrbd/gateway/service"
)

const (
	DefaultGatewayFleetPageSize int64 = 128
	MaxGatewayFleetPageSize     int64 = 512
)

type GatewayFleetListOptions struct {
	Limit    int64
	Cursor   string
	Revision int64
}

type GatewayFleetPage struct {
	Records    []service.GatewayRecord `json:"records"`
	Revision   int64                   `json:"revision"`
	NextCursor string                  `json:"next_cursor,omitempty"`
}

type GatewayFleetEventType string

const (
	GatewayFleetEventPut            GatewayFleetEventType = "put"
	GatewayFleetEventDelete         GatewayFleetEventType = "delete"
	GatewayFleetEventResyncRequired GatewayFleetEventType = "resync_required"
)

type GatewayFleetEvent struct {
	Type            GatewayFleetEventType  `json:"type"`
	GatewayID       string                 `json:"gateway_id,omitempty"`
	Record          *service.GatewayRecord `json:"record,omitempty"`
	Revision        int64                  `json:"revision"`
	CompactRevision int64                  `json:"compact_revision,omitempty"`
	Reason          string                 `json:"reason,omitempty"`
}

// ListGatewayFleetPage returns one revision-pinned page. Callers must carry
// both Revision and NextCursor into the next request so a changing fleet cannot
// produce a mixed snapshot.
func (r *EtcdRepository) ListGatewayFleetPage(ctx context.Context, opts GatewayFleetListOptions) (GatewayFleetPage, error) {
	if r == nil || r.client == nil {
		return GatewayFleetPage{}, errNoEtcdClient
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultGatewayFleetPageSize
	}
	if limit > MaxGatewayFleetPageSize {
		return GatewayFleetPage{}, fmt.Errorf("gateway fleet page limit %d exceeds maximum %d", limit, MaxGatewayFleetPageSize)
	}
	prefix := r.gatewayPrefix()
	start := prefix
	if opts.Cursor != "" {
		if !strings.HasPrefix(opts.Cursor, prefix) {
			return GatewayFleetPage{}, fmt.Errorf("gateway fleet cursor is outside registry prefix")
		}
		start = opts.Cursor + "\x00"
	}
	getOpts := []clientv3.OpOption{
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)),
		clientv3.WithLimit(limit),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}
	if opts.Revision > 0 {
		getOpts = append(getOpts, clientv3.WithRev(opts.Revision))
	}
	r.pressure.countPrefixScan()
	resp, err := r.client.Get(ctx, start, getOpts...)
	r.observeEtcd(err)
	if err != nil {
		return GatewayFleetPage{}, err
	}
	return decodeGatewayFleetPage(prefix, resp)
}

func decodeGatewayFleetPage(prefix string, resp *clientv3.GetResponse) (GatewayFleetPage, error) {
	page := GatewayFleetPage{Records: []service.GatewayRecord{}}
	if resp == nil {
		return page, nil
	}
	if resp.Header != nil {
		page.Revision = resp.Header.Revision
	}
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, "/status") {
			continue
		}
		var rec service.GatewayRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			return GatewayFleetPage{}, fmt.Errorf("decode gateway fleet record %q: %w", key, err)
		}
		rec = service.NormalizeGatewayFleetRecord(rec)
		rec.RegistryRevision = kv.ModRevision
		if err := service.ValidateGatewayFleetRecord(rec); err != nil {
			return GatewayFleetPage{}, fmt.Errorf("validate gateway fleet record %q: %w", key, err)
		}
		page.Records = append(page.Records, rec)
	}
	if resp.More && len(resp.Kvs) > 0 {
		page.NextCursor = string(resp.Kvs[len(resp.Kvs)-1].Key)
	}
	return page, nil
}

// WatchGatewayFleet resumes strictly after a list/watch checkpoint. A canceled
// or compacted feed emits resync_required and closes; the caller then obtains a
// fresh bounded snapshot instead of guessing across a revision gap.
func (r *EtcdRepository) WatchGatewayFleet(ctx context.Context, afterRevision int64) (<-chan GatewayFleetEvent, error) {
	if r == nil || r.client == nil {
		return nil, errNoEtcdClient
	}
	if afterRevision < 0 {
		return nil, fmt.Errorf("gateway fleet watch revision must not be negative")
	}
	opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithPrevKV()}
	if afterRevision > 0 {
		opts = append(opts, clientv3.WithRev(afterRevision+1))
	}
	watch := r.client.Watch(ctx, r.gatewayPrefix(), opts...)
	out := make(chan GatewayFleetEvent, 16)
	go func() {
		defer close(out)
		for resp := range watch {
			if resp.Canceled || resp.Err() != nil {
				reason := "watch canceled"
				if err := resp.Err(); err != nil {
					reason = err.Error()
				}
				out <- GatewayFleetEvent{
					Type: GatewayFleetEventResyncRequired, Revision: resp.Header.Revision,
					CompactRevision: resp.CompactRevision, Reason: reason,
				}
				return
			}
			events, err := decodeGatewayFleetWatchResponse(r.gatewayPrefix(), resp)
			if err != nil {
				out <- GatewayFleetEvent{Type: GatewayFleetEventResyncRequired, Revision: resp.Header.Revision, Reason: err.Error()}
				return
			}
			for _, event := range events {
				select {
				case <-ctx.Done():
					return
				case out <- event:
				}
			}
		}
	}()
	return out, nil
}

func decodeGatewayFleetWatchResponse(prefix string, resp clientv3.WatchResponse) ([]GatewayFleetEvent, error) {
	out := make([]GatewayFleetEvent, 0, len(resp.Events))
	for _, event := range resp.Events {
		if event == nil || event.Kv == nil {
			return nil, fmt.Errorf("gateway fleet watch returned an empty event")
		}
		gatewayID, ok := gatewayIDFromFleetStatusKey(prefix, string(event.Kv.Key))
		if !ok {
			continue
		}
		revision := event.Kv.ModRevision
		if revision == 0 {
			revision = resp.Header.Revision
		}
		if event.Type == clientv3.EventTypeDelete {
			out = append(out, GatewayFleetEvent{Type: GatewayFleetEventDelete, GatewayID: gatewayID, Revision: revision})
			continue
		}
		var rec service.GatewayRecord
		if err := json.Unmarshal(event.Kv.Value, &rec); err != nil {
			return nil, fmt.Errorf("decode gateway fleet record %q: %w", gatewayID, err)
		}
		rec = service.NormalizeGatewayFleetRecord(rec)
		rec.RegistryRevision = revision
		if err := service.ValidateGatewayFleetRecord(rec); err != nil {
			return nil, fmt.Errorf("validate gateway fleet record %q: %w", gatewayID, err)
		}
		out = append(out, GatewayFleetEvent{Type: GatewayFleetEventPut, GatewayID: gatewayID, Record: &rec, Revision: revision})
	}
	return out, nil
}

func gatewayIDFromFleetStatusKey(prefix, key string) (string, bool) {
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, "/status") {
		return "", false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(key, prefix), "/status")
	if id == "" || strings.Contains(id, "/") {
		return "", false
	}
	return id, true
}
