//go:build !legacy_redis
// +build !legacy_redis

package store

import (
	"context"
	"fmt"
	"time"
)

// RedisStore is a legacy object-store implementation. It is disabled in the
// default build to keep Phase H docs/ops surface focused on SBS (`sbs-data`).
//
// Build with `-tags legacy_redis` to enable the real implementation.
type RedisStore struct {
	Addr    string
	Timeout time.Duration
}

func NewRedisStore(addr string, timeout time.Duration) *RedisStore {
	return &RedisStore{Addr: addr, Timeout: timeout}
}

func (r *RedisStore) disabledErr() error {
	return fmt.Errorf("redis object store is disabled in this build (build with -tags legacy_redis to enable)")
}

func (r *RedisStore) Get(_ context.Context, _ string) ([]byte, bool, error) {
	return nil, false, r.disabledErr()
}
func (r *RedisStore) Set(_ context.Context, _ string, _ []byte) error { return r.disabledErr() }
func (r *RedisStore) Put(_ context.Context, _ string, _ []byte) error { return r.disabledErr() }
func (r *RedisStore) Delete(_ context.Context, _ string) error        { return r.disabledErr() }
func (r *RedisStore) List(_ context.Context, _, _ string, _ int) ([]string, string, error) {
	return nil, "", r.disabledErr()
}
