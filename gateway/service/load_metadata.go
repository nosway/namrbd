package service

import (
	"context"
	"strings"
)

type loadMetadataContextKey struct{}
type readPathAttributionContextKey struct{}

type LoadMetadata struct {
	Index string
	Phase string
}

func WithLoadMetadata(ctx context.Context, index, phase string) context.Context {
	index = strings.TrimSpace(index)
	phase = strings.TrimSpace(phase)
	if index == "" && phase == "" {
		return ctx
	}
	return context.WithValue(ctx, loadMetadataContextKey{}, LoadMetadata{
		Index: index,
		Phase: phase,
	})
}

func LoadMetadataFromContext(ctx context.Context) LoadMetadata {
	if ctx == nil {
		return LoadMetadata{}
	}
	meta, _ := ctx.Value(loadMetadataContextKey{}).(LoadMetadata)
	return meta
}

func WithReadPathAttribution(ctx context.Context, enabled bool) context.Context {
	if !enabled {
		return ctx
	}
	return context.WithValue(ctx, readPathAttributionContextKey{}, true)
}

func ReadPathAttributionFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	enabled, _ := ctx.Value(readPathAttributionContextKey{}).(bool)
	return enabled
}
