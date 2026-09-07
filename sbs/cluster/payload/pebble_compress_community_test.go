//go:build !enterprise

package payload

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/nosway/namrbd/sbs/cluster/payload/compress"
)

func TestCommunityBuildRejectsInlineCompressionActivation(t *testing.T) {
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "payload-community"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SetCompressionCodec(compress.CodecZSTD); err == nil {
		t.Fatal("Community build activated Enterprise inline compression")
	}
	encoded := make([]byte, compress.HeaderSize)
	binary.BigEndian.PutUint64(encoded[:8], compress.HeaderMagic)
	if err := store.db.Set([]byte("volume-a:chk:enterprise"), encoded, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(context.Background(), "volume-a:chk:enterprise"); found || !errors.Is(err, compress.ErrEnterpriseOnly) {
		t.Fatalf("Community read of Enterprise payload found=%v err=%v", found, err)
	}
}
