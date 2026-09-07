//go:build !enterprise

package main

import (
	"context"
	"fmt"

	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

func configureEnterpriseWORMGuard(_ context.Context, _ *clustermeta.Repository, _ clustermeta.KV, _ string, enabled bool) (enterpriseWORMLifecycle, error) {
	if enabled {
		return nil, fmt.Errorf("NAMRBD_ENTERPRISE_WORM requires an Enterprise Edition build")
	}
	return nil, nil
}
