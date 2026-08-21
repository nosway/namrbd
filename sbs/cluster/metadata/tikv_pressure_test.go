package metadata

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The caller supplies the key set, so BatchGet is the only place its size can
// be capped. Without chunking a raised write-effects batch turns one request
// into thousands of keys.
func TestChunkBatchGetKeysBoundsBatchSize(t *testing.T) {
	mk := func(n int) [][]byte {
		out := make([][]byte, n)
		for i := range out {
			out[i] = []byte{byte(i)}
		}
		return out
	}
	for _, tc := range []struct{ keys, max, wantChunks int }{
		{0, 128, 1},
		{1, 128, 1},
		{128, 128, 1},
		{129, 128, 2},
		{1000, 128, 8},
		{2000, 128, 16},
	} {
		got := chunkBatchGetKeys(mk(tc.keys), tc.max)
		if len(got) != tc.wantChunks {
			t.Errorf("%d keys at max %d produced %d chunks, want %d", tc.keys, tc.max, len(got), tc.wantChunks)
		}
		total := 0
		for _, c := range got {
			if len(c) > tc.max {
				t.Errorf("a chunk carried %d keys, above the bound of %d", len(c), tc.max)
			}
			total += len(c)
		}
		if total != tc.keys {
			t.Errorf("chunking %d keys yielded %d; keys were lost or duplicated", tc.keys, total)
		}
	}
}

// Chunking rather than rejecting is deliberate: failing an oversized request
// would turn a pressure problem into an availability one.
func TestChunkBatchGetKeysNeverDropsWork(t *testing.T) {
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
	seen := map[string]bool{}
	for _, chunk := range chunkBatchGetKeys(keys, 2) {
		for _, k := range chunk {
			if seen[string(k)] {
				t.Errorf("key %q appeared in more than one chunk", k)
			}
			seen[string(k)] = true
		}
	}
	if len(seen) != len(keys) {
		t.Errorf("%d of %d keys survived chunking", len(seen), len(keys))
	}
}

// A non-positive bound must not produce an infinite loop or drop keys.
func TestChunkBatchGetKeysHandlesDegenerateBound(t *testing.T) {
	keys := [][]byte{[]byte("a"), []byte("b")}
	for _, max := range []int{0, -1} {
		got := chunkBatchGetKeys(keys, max)
		if len(got) != 1 || len(got[0]) != len(keys) {
			t.Errorf("bound %d produced %v", max, got)
		}
	}
}

func TestTiKVPressureCountersRecordAndReset(t *testing.T) {
	ResetTiKVPressureForTest()
	if got := TiKVPressureSnapshotNow(); got != (TiKVPressureSnapshot{}) {
		t.Fatalf("counters did not start at zero: %+v", got)
	}
	tikvPressure.batchGets.Add(1)
	tikvPressure.batchGetKeys.Add(300)
	tikvPressure.batchGetChunks.Add(3)
	tikvPressure.pointGets.Add(2)
	tikvPressure.txnRetries.Add(1)

	got := TiKVPressureSnapshotNow()
	if got.BatchGetCount != 1 || got.BatchGetKeyCount != 300 || got.BatchGetChunkCount != 3 {
		t.Errorf("batch counters = %+v", got)
	}
	if got.PointGetCount != 2 || got.TxnRetryCount != 1 {
		t.Errorf("point and retry counters = %+v", got)
	}
	// The scan counter must stay at zero: nothing in this package scans.
	if got.FullScanCount != 0 {
		t.Errorf("a full scan was counted: %+v", got)
	}
	ResetTiKVPressureForTest()
	if got := TiKVPressureSnapshotNow(); got != (TiKVPressureSnapshot{}) {
		t.Errorf("reset left %+v", got)
	}
}

// TiKV key enumeration is confined to the two List methods. Both normalize the
// caller's limit to a hard page cap before opening an iterator; any iterator or
// scanner added elsewhere remains a test failure.
func TestTiKVAccessOnlyUsesBoundedListPages(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "tikv.go", nil, 0)
	if err != nil {
		t.Fatalf("parse tikv.go: %v", err)
	}
	allowed := map[string]bool{
		"tiKVTxnKV.List": true,
		"tiKVTxn.List":   true,
	}
	iteratorCalls := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		method := tikvMethodName(fn)
		bounded := false
		methodIteratorCalls := 0
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch target := call.Fun.(type) {
			case *ast.Ident:
				if target.Name == "boundedTiKVListLimit" {
					bounded = true
				}
			case *ast.SelectorExpr:
				switch target.Sel.Name {
				case "Scan", "Iter", "IterReverse", "NewScanner":
					methodIteratorCalls++
					iteratorCalls++
				}
			}
			return true
		})
		if methodIteratorCalls == 0 {
			continue
		}
		if !allowed[method] {
			t.Errorf("%s introduces %d TiKV iterator/scanner call(s) outside the bounded List API", method, methodIteratorCalls)
		}
		if !bounded {
			t.Errorf("%s opens a TiKV iterator without boundedTiKVListLimit", method)
		}
	}
	if iteratorCalls != len(allowed) {
		t.Fatalf("TiKV iterator calls=%d want %d bounded List implementations", iteratorCalls, len(allowed))
	}
}

func tikvMethodName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return fn.Name.Name
	}
	receiver := fn.Recv.List[0].Type
	if pointer, ok := receiver.(*ast.StarExpr); ok {
		receiver = pointer.X
	}
	if ident, ok := receiver.(*ast.Ident); ok {
		return ident.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

func TestBoundedTiKVListLimit(t *testing.T) {
	for _, tc := range []struct {
		input int
		want  int
	}{
		{input: -1, want: defaultTiKVListKeys},
		{input: 0, want: defaultTiKVListKeys},
		{input: 1, want: 1},
		{input: MaxTiKVListKeys, want: MaxTiKVListKeys},
		{input: MaxTiKVListKeys + 1, want: MaxTiKVListKeys},
	} {
		if got := boundedTiKVListLimit(tc.input); got != tc.want {
			t.Errorf("boundedTiKVListLimit(%d)=%d want %d", tc.input, got, tc.want)
		}
	}
	if MaxTiKVListKeys != MembershipProjectionPageMaximum {
		t.Errorf("MaxTiKVListKeys=%d projection maximum=%d", MaxTiKVListKeys, MembershipProjectionPageMaximum)
	}
}

// The bound must match the budget the entry plan names, or the two drift.
func TestMaxBatchGetKeysMatchesTheBudget(t *testing.T) {
	if MaxBatchGetKeys != 128 {
		t.Errorf("MaxBatchGetKeys is %d; the t2_large budget names 128", MaxBatchGetKeys)
	}
}
