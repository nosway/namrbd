//go:build !enterprise

package driver

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCommunityRejectsECVolumeParameters(t *testing.T) {
	_, err := parseVolumeParameters(map[string]string{
		"redundancy_backend": "ec",
		"ec_profile":         "ec-6-3",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("community EC err=%v want InvalidArgument", err)
	}
}
