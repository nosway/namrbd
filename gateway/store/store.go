package store

import "context"

const BlockSize = 4096

type KV interface {
	Get(ctx context.Context, key string) (value []byte, found bool, err error)
	Set(ctx context.Context, key string, value []byte) error
}

type ObjectStore interface {
	Get(ctx context.Context, key string) (value []byte, found bool, err error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix, cursor string, limit int) (keys []string, nextCursor string, err error)
}

type Volume struct {
	ID        uint64
	Prefix    string
	SizeBytes uint64
}

func BuildBlockKey(prefix string, blockNo uint32) string {
	return prefix + ";" + lowerHex8(blockNo)
}

func BuildChunkKey(prefix string, chunkID uint64) string {
	return prefix + ":chk:" + formatUint(chunkID)
}

func lowerHex8(v uint32) string {
	const hex = "0123456789abcdef"
	var out [8]byte
	for i := 7; i >= 0; i-- {
		out[i] = hex[v&0xF]
		v >>= 4
	}
	return string(out[:])
}

func formatUint(v uint64) string {
	if v == 0 {
		return "0"
	}
	var out [20]byte
	i := len(out)
	for v > 0 {
		i--
		out[i] = byte('0' + (v % 10))
		v /= 10
	}
	return string(out[i:])
}
