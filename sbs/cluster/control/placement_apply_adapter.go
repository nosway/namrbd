package control

import (
	"context"
	"fmt"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

// PlacementApplyAdapter is the draft internal authority boundary that an
// sbs-service-owned component could expose to gateway/maintenance callers.
type PlacementApplyAdapter interface {
	ApplyPlacementChanges(ctx context.Context, req metadata.PlacementApplyRequest) error
}

// PlacementApplyInternalService is the minimal service shape that an
// sbs-service-owned internal authority could expose behind an adapter.
type PlacementApplyInternalService interface {
	ApplyPlacementChanges(ctx context.Context, req metadata.PlacementApplyRequest) error
}

type RepositoryBackedPlacementApplyInternalService struct {
	service *metadata.PlacementApplyService
}

func NewRepositoryBackedPlacementApplyInternalService(store metadata.PlacementApplyAuthority) *RepositoryBackedPlacementApplyInternalService {
	return &RepositoryBackedPlacementApplyInternalService{
		service: metadata.NewPlacementApplyService(store),
	}
}

func (s *RepositoryBackedPlacementApplyInternalService) ApplyPlacementChanges(ctx context.Context, req metadata.PlacementApplyRequest) error {
	return s.service.ApplyPlacementChanges(ctx, req)
}

type RepositoryBackedPlacementApplyAdapter struct {
	service PlacementApplyInternalService
}

func NewRepositoryBackedPlacementApplyAdapter(store metadata.PlacementApplyAuthority) *RepositoryBackedPlacementApplyAdapter {
	return &RepositoryBackedPlacementApplyAdapter{
		service: NewRepositoryBackedPlacementApplyInternalService(store),
	}
}

func (a *RepositoryBackedPlacementApplyAdapter) ApplyPlacementChanges(ctx context.Context, req metadata.PlacementApplyRequest) error {
	return a.service.ApplyPlacementChanges(ctx, req)
}

type ServiceBackedPlacementApplyAdapter struct {
	service PlacementApplyInternalService
}

func NewServiceBackedPlacementApplyAdapter(service PlacementApplyInternalService) *ServiceBackedPlacementApplyAdapter {
	return &ServiceBackedPlacementApplyAdapter{service: service}
}

func (a *ServiceBackedPlacementApplyAdapter) ApplyPlacementChanges(ctx context.Context, req metadata.PlacementApplyRequest) error {
	if a.service == nil {
		return fmt.Errorf("placement apply internal service is required")
	}
	if err := req.Validate(); err != nil {
		return err
	}
	return a.service.ApplyPlacementChanges(ctx, req)
}

type TimeoutPlacementApplyAdapter struct {
	next    PlacementApplyAdapter
	timeout time.Duration
}

func NewTimeoutPlacementApplyAdapter(next PlacementApplyAdapter, timeout time.Duration) *TimeoutPlacementApplyAdapter {
	return &TimeoutPlacementApplyAdapter{next: next, timeout: timeout}
}

func (a *TimeoutPlacementApplyAdapter) ApplyPlacementChanges(ctx context.Context, req metadata.PlacementApplyRequest) error {
	if a.next == nil {
		return fmt.Errorf("placement apply adapter is required")
	}
	if a.timeout <= 0 {
		return a.next.ApplyPlacementChanges(ctx, req)
	}
	callCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	return a.next.ApplyPlacementChanges(callCtx, req)
}
