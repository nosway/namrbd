package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

type leaderLeaseRecord struct {
	NodeID         string `json:"node_id"`
	AcquiredAtUnix int64  `json:"acquired_at_unix"`
	ExpiresAtUnix  int64  `json:"expires_at_unix"`
}

type leaderLeaseManager struct {
	kv     clustermeta.KV
	key    string
	nodeID string

	leaseDuration time.Duration
	renewInterval time.Duration
	now           func() time.Time

	isLeader atomic.Bool

	mu      sync.RWMutex
	current leaderLeaseRecord
	lastErr error
}

func newLeaderLeaseManager(kv clustermeta.KV, root, nodeID string) *leaderLeaseManager {
	return &leaderLeaseManager{
		kv:            kv,
		key:           leaderLeaseKey(root),
		nodeID:        nodeID,
		leaseDuration: 10 * time.Second,
		renewInterval: 3 * time.Second,
		now:           time.Now,
	}
}

func (m *leaderLeaseManager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.renewInterval)
	defer ticker.Stop()
	defer m.isLeader.Store(false)
	for {
		m.refresh(ctx)
		select {
		case <-ctx.Done():
			releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = m.Release(releaseCtx)
			cancel()
			return
		case <-ticker.C:
		}
	}
}

func (m *leaderLeaseManager) refresh(ctx context.Context) {
	record, leader, err := m.acquireOrRenew(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.lastErr = err
		m.current = record
		m.isLeader.Store(false)
		return
	}
	m.lastErr = nil
	m.current = record
	m.isLeader.Store(leader)
}

func (m *leaderLeaseManager) acquireOrRenew(ctx context.Context) (leaderLeaseRecord, bool, error) {
	var next leaderLeaseRecord
	var leader bool
	err := clustermeta.RunInTransaction(ctx, m.kv, func(tx clustermeta.ReadWriter) error {
		now := m.now().UTC()
		raw, found, err := tx.Get(ctx, m.key)
		if err != nil {
			return err
		}
		if !found {
			next = leaderLeaseRecord{
				NodeID:         m.nodeID,
				AcquiredAtUnix: now.Unix(),
				ExpiresAtUnix:  now.Add(m.leaseDuration).Unix(),
			}
			leader = true
			return putLeaderLease(ctx, tx, m.key, next)
		}

		var current leaderLeaseRecord
		if err := json.Unmarshal(raw, &current); err != nil {
			return err
		}
		expired := current.ExpiresAtUnix <= now.Unix()
		if current.NodeID == m.nodeID || expired {
			acquiredAt := current.AcquiredAtUnix
			if current.NodeID != m.nodeID || acquiredAt == 0 {
				acquiredAt = now.Unix()
			}
			next = leaderLeaseRecord{
				NodeID:         m.nodeID,
				AcquiredAtUnix: acquiredAt,
				ExpiresAtUnix:  now.Add(m.leaseDuration).Unix(),
			}
			leader = true
			return putLeaderLease(ctx, tx, m.key, next)
		}
		next = current
		leader = false
		return nil
	})
	return next, leader, err
}

func (m *leaderLeaseManager) IsLeader() bool {
	return m.isLeader.Load()
}

func (m *leaderLeaseManager) Snapshot() (leaderLeaseRecord, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current, m.isLeader.Load(), m.lastErr
}

func (m *leaderLeaseManager) CurrentLeader(ctx context.Context) (leaderLeaseRecord, error) {
	record, _, err := m.Snapshot()
	if err == nil && record.NodeID != "" && !m.isExpired(record) {
		return record, nil
	}
	raw, found, err := m.kv.Get(ctx, m.key)
	if err != nil {
		return leaderLeaseRecord{}, err
	}
	if !found {
		return leaderLeaseRecord{}, clustermeta.ErrNotFound
	}
	var current leaderLeaseRecord
	if err := json.Unmarshal(raw, &current); err != nil {
		return leaderLeaseRecord{}, err
	}
	if m.isExpired(current) {
		return leaderLeaseRecord{}, clustermeta.ErrNotFound
	}
	return current, nil
}

func (m *leaderLeaseManager) Release(ctx context.Context) error {
	record, leader, err := m.Snapshot()
	if err != nil || !leader || record.NodeID != m.nodeID {
		return nil
	}
	err = clustermeta.RunInTransaction(ctx, m.kv, func(tx clustermeta.ReadWriter) error {
		raw, found, err := tx.Get(ctx, m.key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		var current leaderLeaseRecord
		if err := json.Unmarshal(raw, &current); err != nil {
			return err
		}
		if current.NodeID != m.nodeID {
			return nil
		}
		return tx.Delete(ctx, m.key)
	})
	if err == nil {
		m.mu.Lock()
		m.current = leaderLeaseRecord{}
		m.lastErr = nil
		m.mu.Unlock()
		m.isLeader.Store(false)
	}
	return err
}

func (m *leaderLeaseManager) MutationReady() bool {
	record, leader, err := m.Snapshot()
	if err != nil || !leader || record.NodeID != m.nodeID {
		return false
	}
	return !m.isExpired(record)
}

func (m *leaderLeaseManager) State() string {
	record, leader, err := m.Snapshot()
	switch {
	case err != nil:
		return "unavailable"
	case leader && record.NodeID == m.nodeID && !m.isExpired(record):
		return "leader"
	case record.NodeID != "" && !m.isExpired(record):
		return "standby"
	default:
		return "election"
	}
}

func (m *leaderLeaseManager) isExpired(record leaderLeaseRecord) bool {
	if record.ExpiresAtUnix == 0 {
		return true
	}
	return record.ExpiresAtUnix <= m.now().UTC().Unix()
}

func putLeaderLease(ctx context.Context, tx clustermeta.ReadWriter, key string, record leaderLeaseRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return tx.Set(ctx, key, raw)
}

func leaderLeaseKey(root string) string {
	return fmt.Sprintf("%s/admin/leader-lease", root)
}

func (s *server) requireLeader(ctx context.Context) error {
	if s.leader != nil && s.leader.IsLeader() {
		return nil
	}
	leaderID := ""
	if s.leader != nil {
		record, _, err := s.leader.Snapshot()
		if err == nil {
			leaderID = record.NodeID
		}
		if leaderID == "" {
			if current, currentErr := s.leader.CurrentLeader(ctx); currentErr == nil {
				leaderID = current.NodeID
			}
		}
	}
	if leaderID == "" {
		return errors.New("sbs-service leader is not available")
	}
	return fmt.Errorf("local node is not leader; current leader=%s", leaderID)
}
