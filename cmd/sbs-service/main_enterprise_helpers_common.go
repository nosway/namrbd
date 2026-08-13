//go:build !enterprise

package main

import (
	"context"
	"encoding/json"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type backupJSONStore interface {
	Get(context.Context, string) ([]byte, bool, error)
	Set(context.Context, string, []byte) error
}

func (s *server) backgroundBudgetSummaries(context.Context, maintenanceSnapshot) []*adminv1.BackgroundWorkBudgetSummary {
	return nil
}

func putBackupJSON(ctx context.Context, kv backupJSONStore, key string, record any) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return kv.Set(ctx, key, payload)
}

func getBackupJSON[T any](ctx context.Context, kv backupJSONStore, key string) (T, error) {
	var zero T
	payload, ok, err := kv.Get(ctx, key)
	if err != nil {
		return zero, err
	}
	if !ok {
		return zero, clustermeta.ErrNotFound
	}
	var out T
	if err := json.Unmarshal(payload, &out); err != nil {
		return zero, err
	}
	return out, nil
}

func notFoundStatus(err error, msg string) error {
	if errorsIsNotFound(err) {
		return status.Error(codes.NotFound, msg)
	}
	return status.Errorf(codes.Internal, "%s: %v", msg, err)
}

func errorsIsNotFound(err error) bool {
	return err == clustermeta.ErrNotFound
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
