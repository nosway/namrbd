package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/service"
)

type staticVolumeLookup struct {
	specs map[uint64]service.VolumeSpec
	calls int
}

func (l *staticVolumeLookup) GetVolume(_ context.Context, volumeID uint64) (service.VolumeSpec, error) {
	l.calls++
	spec, ok := l.specs[volumeID]
	if !ok {
		return service.VolumeSpec{}, service.ErrVolumeNotFound
	}
	return spec, nil
}

func TestClientLookupVolumeCachesGeometryPerVolume(t *testing.T) {
	lookup := &staticVolumeLookup{specs: map[uint64]service.VolumeSpec{
		0x7b: {
			ID:              service.HexVolumeID(0x7b),
			Name:            "small",
			SizeBytes:       1 << 20,
			BlockSize:       4096,
			ChunkSizeBytes:  64 << 10,
			ExtentPageBytes: 256 << 10,
		},
		0x7c: {
			ID:              service.HexVolumeID(0x7c),
			Name:            "large",
			SizeBytes:       1 << 20,
			BlockSize:       4096,
			ChunkSizeBytes:  64 << 10,
			ExtentPageBytes: 4 << 20,
		},
	}}
	client := &Client{
		volumeLookup:   lookup,
		volumeCacheTTL: time.Hour,
		volumeCache:    make(map[string]cachedVolumeSpec),
	}

	first, err := client.lookupVolume(context.Background(), "0000007b")
	if err != nil {
		t.Fatalf("lookup first volume: %v", err)
	}
	if first.ExtentPageBytes != 256<<10 || first.ChunkSizeBytes != 64<<10 {
		t.Fatalf("first volume geometry=%d/%d", first.ExtentPageBytes, first.ChunkSizeBytes)
	}

	second, err := client.lookupVolume(context.Background(), "0000007c")
	if err != nil {
		t.Fatalf("lookup second volume: %v", err)
	}
	if second.ExtentPageBytes != 4<<20 || second.ChunkSizeBytes != 64<<10 {
		t.Fatalf("second volume geometry=%d/%d", second.ExtentPageBytes, second.ChunkSizeBytes)
	}

	lookup.specs[0x7b] = service.VolumeSpec{
		ID:              service.HexVolumeID(0x7b),
		Name:            "small",
		SizeBytes:       1 << 20,
		BlockSize:       4096,
		ChunkSizeBytes:  64 << 10,
		ExtentPageBytes: 512 << 10,
	}
	cached, err := client.lookupVolume(context.Background(), "0000007b")
	if err != nil {
		t.Fatalf("lookup cached volume: %v", err)
	}
	if cached.ExtentPageBytes != 256<<10 {
		t.Fatalf("cached extent page bytes=%d want=%d", cached.ExtentPageBytes, 256<<10)
	}
	if lookup.calls != 2 {
		t.Fatalf("lookup calls=%d want=2", lookup.calls)
	}
}
