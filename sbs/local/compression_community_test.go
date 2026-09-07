//go:build !enterprise

package local

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/nosway/namrbd/gateway/service"
)

func TestCommunityRejectsEnterpriseCompressionPolicy(t *testing.T) {
	client, err := Open(Config{Path: filepath.Join(t.TempDir(), "pebble")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()
	_, err = client.ApplyCompressionPolicy(context.Background(), &service.ApplyCompressionPolicyRequest{Policy: service.CompressionPolicy{
		VolumeID: "00000065", PolicyID: "zstd-a", PolicyRevision: 1,
		Codec: "ZSTD", ChecksumEnabled: true, Enabled: true,
	}})
	if err == nil {
		t.Fatal("Community accepted Enterprise compression policy")
	}
}
