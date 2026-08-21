package metadata

import "sync/atomic"

// MaxBatchGetKeys bounds how many keys one TiKV BatchGet may carry.
//
// AA-IMPL-003A found that TiKV data-path access is point and batch only. The
// unbounded dimension is the caller supplying the key set: BatchGet passed
// whatever it was given straight to TiKV. The default write-effects batch of
// 16 produces 32 keys, comfortably inside this bound, but nothing prevented an operator raising
// sbs_service.write_effects.batch_max and turning one request into thousands of
// keys. The entry plan budget for t2_large names 128.
const MaxBatchGetKeys = 128

// MaxTiKVListKeys bounds the control-plane projection and registry pages that
// enumerate keys. Every iterator is admitted only through a List method that
// applies this cap; callers must follow the returned cursor for larger views.
const MaxTiKVListKeys = 512

const defaultTiKVListKeys = 128

func boundedTiKVListLimit(limit int) int {
	if limit <= 0 {
		return defaultTiKVListKeys
	}
	if limit > MaxTiKVListKeys {
		return MaxTiKVListKeys
	}
	return limit
}

// TiKVPressure counts what this process asks of TiKV.
type TiKVPressure struct {
	batchGets      atomic.Int64
	batchGetKeys   atomic.Int64
	batchGetChunks atomic.Int64
	pointGets      atomic.Int64
	// fullScans stays at zero. Bounded List pages are not full scans; an
	// unbounded enumeration introduced later must increment this counter.
	fullScans  atomic.Int64
	txnRetries atomic.Int64
}

var tikvPressure TiKVPressure

// TiKVPressureSnapshot is the observable form.
type TiKVPressureSnapshot struct {
	BatchGetCount      int64 `json:"tikv_batch_get_count"`
	BatchGetKeyCount   int64 `json:"tikv_batch_get_key_count"`
	BatchGetChunkCount int64 `json:"tikv_batch_get_chunk_count"`
	PointGetCount      int64 `json:"tikv_point_get_count"`
	FullScanCount      int64 `json:"tikv_full_scan_count"`
	TxnRetryCount      int64 `json:"tikv_txn_retry_count"`
}

// TiKVPressureSnapshotNow returns the current counts.
func TiKVPressureSnapshotNow() TiKVPressureSnapshot {
	return TiKVPressureSnapshot{
		BatchGetCount:      tikvPressure.batchGets.Load(),
		BatchGetKeyCount:   tikvPressure.batchGetKeys.Load(),
		BatchGetChunkCount: tikvPressure.batchGetChunks.Load(),
		PointGetCount:      tikvPressure.pointGets.Load(),
		FullScanCount:      tikvPressure.fullScans.Load(),
		TxnRetryCount:      tikvPressure.txnRetries.Load(),
	}
}

// ResetTiKVPressureForTest zeroes the counters. Tests call it; nothing else
// should.
func ResetTiKVPressureForTest() {
	tikvPressure.batchGets.Store(0)
	tikvPressure.batchGetKeys.Store(0)
	tikvPressure.batchGetChunks.Store(0)
	tikvPressure.pointGets.Store(0)
	tikvPressure.fullScans.Store(0)
	tikvPressure.txnRetries.Store(0)
}

// chunkBatchGetKeys splits a key set into batches no larger than
// MaxBatchGetKeys.
//
// Chunking rather than rejecting is deliberate: a caller with more keys than
// the bound still has legitimate work to do, and failing the request would turn
// a pressure problem into an availability one. The chunk count is recorded so
// an oversized caller is visible.
func chunkBatchGetKeys(keys [][]byte, max int) [][][]byte {
	if max <= 0 || len(keys) <= max {
		return [][][]byte{keys}
	}
	var out [][][]byte
	for start := 0; start < len(keys); start += max {
		end := start + max
		if end > len(keys) {
			end = len(keys)
		}
		out = append(out, keys[start:end])
	}
	return out
}

// ChunkBatchGetKeysForTest exposes the chunker to the synthetic scale harness,
// which drives it at tier scale rather than reimplementing the bound.
func ChunkBatchGetKeysForTest(keys [][]byte, max int) [][][]byte {
	return chunkBatchGetKeys(keys, max)
}

// tikvOutcome receives the result of TiKV calls this package already makes.
//
// AA-IMPL-004B feeds the dependency availability tracker from it. It is a
// function rather than a depavail dependency so this package keeps knowing
// nothing about availability policy: it reports what happened, and what that
// means is decided elsewhere.
//
// Reporting from existing calls is the point. A dedicated liveness probe would
// add a standing read per process to learn something the read path already
// knows, which is a fraction of the load AA-IMPL-003 spent three slices
// removing.
var tikvOutcome atomic.Value // func(error)

// SetTiKVOutcomeObserver installs the outcome observer. Passing nil removes it.
func SetTiKVOutcomeObserver(f func(error)) {
	if f == nil {
		tikvOutcome.Store((func(error))(nil))
		return
	}
	tikvOutcome.Store(f)
}

func observeTiKV(err error) {
	v := tikvOutcome.Load()
	if v == nil {
		return
	}
	if f, ok := v.(func(error)); ok && f != nil {
		f(err)
	}
}
