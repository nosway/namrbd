//go:build !enterprise

package driver

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validateEditionVolumeParameters(params VolumeParameters) error {
	if params.RedundancyBackend == redundancyBackendEC {
		return status.Error(codes.InvalidArgument, "redundancy_backend=ec requires the Enterprise edition")
	}
	return nil
}
