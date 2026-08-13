package local

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/structuredlog"
)

func TestClientECShardWriteReadDelete(t *testing.T) {
	ctx := context.Background()
	client, err := Open(Config{Path: t.TempDir(), TraceDataOperations: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "ec-shard-test",
		Prefix:          "ec-shard-test",
		SizeBytes:       1 << 20,
		BlockSize:       4096,
		ChunkSizeBytes:  4096,
		ExtentPageBytes: 1 << 20,
	})
	if _, err := client.CreateVolume(ctx, spec); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	openResp, err := client.OpenVolume(ctx, &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context:    ecShardTestContext("open", ""),
	})
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}

	payload := []byte("ec-shard-payload")
	checksum := ecShardChecksum(payload)
	var logs bytes.Buffer
	restore := structuredlog.SetOutput(&logs)
	defer restore()
	writeResp, err := client.WriteECShard(ctx, &service.WriteECShardRequest{
		VolumeID:         service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle:     openResp.VolumeHandle,
		ObjectID:         "ec:00a1b2c3:0:1",
		StripeID:         "0",
		StripeGeneration: 1,
		ShardID:          2,
		Role:             "coding",
		RoleIndex:        0,
		StoreID:          "node-a/default",
		Data:             payload,
		Checksum:         checksum,
		Context:          ecShardTestContext("write", "idem-write"),
	})
	if err != nil {
		t.Fatalf("WriteECShard: %v", err)
	}
	if writeResp.Checksum != checksum || writeResp.LengthBytes != uint64(len(payload)) {
		t.Fatalf("writeResp=%+v", writeResp)
	}
	for _, want := range []string{
		`"component":"sbs.data"`,
		`"event":"ec_shard_write_completed"`,
		`"request_id":"write"`,
		`"object_id":"ec:00a1b2c3:0:1"`,
		`"stripe_id":"0"`,
		`"stripe_generation":1`,
		`"shard_id":2`,
		`"role":"coding"`,
		`"store_id":"node-a/default"`,
		`"data_write_duration_ms":`,
		`"duration_ms":`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs missing %q: %s", want, logs.String())
		}
	}

	readResp, err := client.ReadECShard(ctx, &service.ReadECShardRequest{
		VolumeID:         service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle:     openResp.VolumeHandle,
		ObjectID:         "ec:00a1b2c3:0:1",
		StripeID:         "0",
		StripeGeneration: 1,
		ShardID:          2,
		StoreID:          "node-a/default",
		OffsetBytes:      3,
		LengthBytes:      5,
		Context:          ecShardTestContext("read", ""),
	})
	if err != nil {
		t.Fatalf("ReadECShard: %v", err)
	}
	if string(readResp.Data) != "shard" {
		t.Fatalf("read data=%q", readResp.Data)
	}

	if _, err := client.DeleteECShard(ctx, &service.DeleteECShardRequest{
		VolumeID:         service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle:     openResp.VolumeHandle,
		ObjectID:         "ec:00a1b2c3:0:1",
		StripeID:         "0",
		StripeGeneration: 1,
		ShardID:          2,
		StoreID:          "node-a/default",
		Context:          ecShardTestContext("delete", "idem-delete"),
	}); err != nil {
		t.Fatalf("DeleteECShard: %v", err)
	}
	if _, err := client.ReadECShard(ctx, &service.ReadECShardRequest{
		VolumeID:         service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle:     openResp.VolumeHandle,
		ObjectID:         "ec:00a1b2c3:0:1",
		StripeID:         "0",
		StripeGeneration: 1,
		ShardID:          2,
		StoreID:          "node-a/default",
		OffsetBytes:      0,
		LengthBytes:      1,
		Context:          ecShardTestContext("read-missing", ""),
	}); err == nil {
		t.Fatal("ReadECShard succeeded after delete")
	}
}

func TestClientECShardWriteRejectsChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	client, err := Open(Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()
	spec := service.NormalizeVolumeSpec(service.VolumeSpec{ID: service.HexVolumeID(0x00a1b2c3), Name: "ec-shard-test", Prefix: "ec-shard-test", SizeBytes: 1 << 20})
	if _, err := client.CreateVolume(ctx, spec); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	openResp, err := client.OpenVolume(ctx, &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context:    ecShardTestContext("open", ""),
	})
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	_, err = client.WriteECShard(ctx, &service.WriteECShardRequest{
		VolumeID:         service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle:     openResp.VolumeHandle,
		ObjectID:         "ec:00a1b2c3:0:1",
		StripeID:         "0",
		StripeGeneration: 1,
		ShardID:          0,
		Role:             "data",
		StoreID:          "default",
		Data:             []byte("payload"),
		Checksum:         "sha256:not-the-payload",
		Context:          ecShardTestContext("write", "idem-write"),
	})
	if err == nil {
		t.Fatal("WriteECShard accepted a checksum mismatch")
	}
}

func TestClientECShardDefaultRouteFallsBackToSingleCustomStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	client, err := Open(Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []StoreSpec{
			{ID: "lowio", Path: filepath.Join(dir, "lowio"), Shards: 1, Weight: 10},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "ec-shard-custom-store-test",
		Prefix:          "ec-shard-custom-store-test",
		SizeBytes:       1 << 20,
		BlockSize:       4096,
		ChunkSizeBytes:  4096,
		ExtentPageBytes: 1 << 20,
	})
	if _, err := client.CreateVolume(ctx, spec); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	openResp, err := client.OpenVolume(ctx, &service.OpenVolumeRequest{
		VolumeID:   service.CanonicalVolumeID(uint64(spec.ID)),
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context:    ecShardTestContext("open-custom-store", ""),
	})
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}

	payload := []byte("ec-shard-custom-store-payload")
	if _, err := client.WriteECShard(ctx, &service.WriteECShardRequest{
		VolumeID:         service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle:     openResp.VolumeHandle,
		ObjectID:         "ec:00a1b2c3:0:1",
		StripeID:         "0",
		StripeGeneration: 1,
		ShardID:          0,
		Role:             "data",
		RoleIndex:        0,
		StoreID:          "node-a/default",
		Data:             payload,
		Checksum:         ecShardChecksum(payload),
		Context:          ecShardTestContext("write-custom-store", "idem-custom-store"),
	}); err != nil {
		t.Fatalf("WriteECShard: %v", err)
	}

	resp, err := client.ReadECShard(ctx, &service.ReadECShardRequest{
		VolumeID:         service.CanonicalVolumeID(uint64(spec.ID)),
		VolumeHandle:     openResp.VolumeHandle,
		ObjectID:         "ec:00a1b2c3:0:1",
		StripeID:         "0",
		StripeGeneration: 1,
		ShardID:          0,
		StoreID:          "node-a/default",
		OffsetBytes:      0,
		LengthBytes:      uint64(len(payload)),
		Context:          ecShardTestContext("read-custom-store", ""),
	})
	if err != nil {
		t.Fatalf("ReadECShard: %v", err)
	}
	if string(resp.Data) != string(payload) {
		t.Fatalf("read data=%q want=%q", resp.Data, payload)
	}
}

func ecShardTestContext(requestID, idempotencyKey string) service.SBSRequestContext {
	return service.SBSRequestContext{
		RequestID:      requestID,
		GatewayID:      "gw-test",
		AttachmentID:   "attach-test",
		Generation:     1,
		IdempotencyKey: idempotencyKey,
	}
}
