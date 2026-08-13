package metadata

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikvconfig "github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/internal/tikvopts"
)

type TiKVConfig struct {
	Options              tikvopts.Options
	Root                 string
	TraceOperations      bool
	EnableAsyncCommit    bool
	EnableOnePhaseCommit bool
}

func OpenTiKVKV(ctx context.Context, cfg TiKVConfig) (KV, func() error, error) {
	if err := tikvopts.Validate(cfg.Options); err != nil {
		return nil, nil, err
	}
	if cfg.Options.APIVersion == tikvopts.APIVersionV1TTL {
		return nil, nil, fmt.Errorf("tikv metadata backend does not support api version %q", cfg.Options.APIVersion)
	}

	restore := tikvconfig.UpdateGlobal(func(conf *tikvconfig.Config) {
		conf.Security = tikvconfig.NewSecurity(
			cfg.Options.TLS.CAPath,
			cfg.Options.TLS.CertPath,
			cfg.Options.TLS.KeyPath,
			nil,
		)
	})
	defer restore()

	clientOpts := []txnkv.ClientOpt{txnkv.WithAPIVersion(parseTxnKVAPIVersion(cfg.Options.APIVersion))}
	if cfg.Options.Keyspace != "" {
		clientOpts = append(clientOpts, txnkv.WithKeyspace(cfg.Options.Keyspace))
	}

	client, err := txnkv.NewClient(cfg.Options.PDEndpoints, clientOpts...)
	if err != nil {
		return nil, nil, err
	}
	kv := &tiKVTxnKV{
		client:               client,
		timeout:              cfg.Options.Timeout,
		traceOperations:      cfg.TraceOperations,
		enableAsyncCommit:    cfg.EnableAsyncCommit,
		enableOnePhaseCommit: cfg.EnableOnePhaseCommit,
	}
	if cfg.Options.Keyspace != "" && cfg.Options.APIVersion != tikvopts.APIVersionV2 {
		return newPrefixedKV(kv, cfg.Options.Keyspace), kv.Close, nil
	}
	return kv, kv.Close, nil
}

func OpenTiKV(ctx context.Context, cfg TiKVConfig) (*Repository, func() error, error) {
	kv, closeFn, err := OpenTiKVKV(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	repo := NewRepository(kv, cfg.Root)
	return repo, closeFn, nil
}

type tiKVTxnKV struct {
	client               *txnkv.Client
	timeout              time.Duration
	traceOperations      bool
	enableAsyncCommit    bool
	enableOnePhaseCommit bool
}

type prefixedKV struct {
	base   KV
	prefix string
}

type prefixedTxn struct {
	base   kvReadWriter
	prefix string
}

func newPrefixedKV(base KV, keyspace string) KV {
	keyspace = strings.Trim(strings.TrimSpace(keyspace), "/")
	if keyspace == "" {
		return base
	}
	return &prefixedKV{
		base:   base,
		prefix: "keyspaces/" + keyspace + "/",
	}
}

func (kv *prefixedKV) prefixed(key string) string {
	return kv.prefix + key
}

func (kv *prefixedKV) unprefixed(key string) string {
	return strings.TrimPrefix(key, kv.prefix)
}

func (kv *prefixedKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return kv.base.Get(ctx, kv.prefixed(key))
}

func (kv *prefixedKV) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	batcher, ok := kv.base.(kvBatchReader)
	if !ok {
		for _, key := range keys {
			value, found, err := kv.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			if found {
				out[key] = append([]byte(nil), value...)
			}
		}
		return out, nil
	}
	prefixedKeys, originalByPrefixed := prefixBatchKeys(kv.prefix, keys)
	values, err := batcher.BatchGet(ctx, prefixedKeys)
	if err != nil {
		return nil, err
	}
	for prefixedKey, value := range values {
		key, ok := originalByPrefixed[prefixedKey]
		if !ok {
			continue
		}
		out[key] = append([]byte(nil), value...)
	}
	return out, nil
}

func (kv *prefixedKV) Set(ctx context.Context, key string, value []byte) error {
	return kv.base.Set(ctx, kv.prefixed(key), value)
}

func (kv *prefixedKV) Delete(ctx context.Context, key string) error {
	return kv.base.Delete(ctx, kv.prefixed(key))
}

func (kv *prefixedKV) List(ctx context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	prefixedCursor := ""
	if cursor != "" {
		prefixedCursor = kv.prefixed(cursor)
	}
	keys, next, err := kv.base.List(ctx, kv.prefixed(prefix), prefixedCursor, limit)
	if err != nil {
		return nil, "", err
	}
	for i, key := range keys {
		keys[i] = kv.unprefixed(key)
	}
	if next != "" {
		next = kv.unprefixed(next)
	}
	return keys, next, nil
}

func (kv *prefixedKV) RunInTransaction(ctx context.Context, fn func(tx kvReadWriter) error) error {
	runner, ok := kv.base.(transactionalKV)
	if !ok {
		return fmt.Errorf("prefixed kv base does not support transactions")
	}
	return runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		return fn(&prefixedTxn{base: tx, prefix: kv.prefix})
	})
}

func (tx *prefixedTxn) prefixed(key string) string {
	return tx.prefix + key
}

func (tx *prefixedTxn) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return tx.base.Get(ctx, tx.prefixed(key))
}

func (tx *prefixedTxn) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	batcher, ok := tx.base.(kvBatchReader)
	if !ok {
		for _, key := range keys {
			value, found, err := tx.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			if found {
				out[key] = append([]byte(nil), value...)
			}
		}
		return out, nil
	}
	prefixedKeys, originalByPrefixed := prefixBatchKeys(tx.prefix, keys)
	values, err := batcher.BatchGet(ctx, prefixedKeys)
	if err != nil {
		return nil, err
	}
	for prefixedKey, value := range values {
		key, ok := originalByPrefixed[prefixedKey]
		if !ok {
			continue
		}
		out[key] = append([]byte(nil), value...)
	}
	return out, nil
}

func (tx *prefixedTxn) Set(ctx context.Context, key string, value []byte) error {
	return tx.base.Set(ctx, tx.prefixed(key), value)
}

func (tx *prefixedTxn) Delete(ctx context.Context, key string) error {
	return tx.base.Delete(ctx, tx.prefixed(key))
}

func prefixBatchKeys(prefix string, keys []string) ([]string, map[string]string) {
	prefixedKeys := make([]string, 0, len(keys))
	originalByPrefixed := make(map[string]string, len(keys))
	for _, key := range keys {
		prefixedKey := prefix + key
		if _, ok := originalByPrefixed[prefixedKey]; ok {
			continue
		}
		originalByPrefixed[prefixedKey] = key
		prefixedKeys = append(prefixedKeys, prefixedKey)
	}
	return prefixedKeys, originalByPrefixed
}

func (kv *tiKVTxnKV) Get(ctx context.Context, key string) (out []byte, found bool, err error) {
	start := time.Now()
	defer func() {
		outcome := "ok"
		if err != nil {
			outcome = "error"
		} else if !found {
			outcome = "not_found"
		}
		logTiKVOperation(kv.traceOperations, "get", "standalone", key, outcome, start, err,
			structuredlog.F("found", found),
		)
	}()
	ctx, cancel := kv.withTimeout(ctx)
	defer cancel()

	txn, err := kv.beginTxnWithTrace("standalone")
	if err != nil {
		return nil, false, err
	}
	value, err := txn.Get(ctx, []byte(key))
	if tikverr.IsErrNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return append([]byte(nil), value...), true, nil
}

func (kv *tiKVTxnKV) Set(ctx context.Context, key string, value []byte) (err error) {
	start := time.Now()
	defer func() {
		logTiKVOperation(kv.traceOperations, "set", "standalone", key, "", start, err)
	}()
	ctx, cancel := kv.withTimeout(ctx)
	defer cancel()

	txn, err := kv.beginTxnWithTrace("standalone")
	if err != nil {
		return err
	}
	if err := txn.Set([]byte(key), append([]byte(nil), value...)); err != nil {
		return err
	}
	return commitTiKVTxnWithTrace(ctx, txn, kv.traceOperations, "standalone", kv.commitProtocolFields()...)
}

func (kv *tiKVTxnKV) Delete(ctx context.Context, key string) (err error) {
	start := time.Now()
	defer func() {
		logTiKVOperation(kv.traceOperations, "delete", "standalone", key, "", start, err)
	}()
	ctx, cancel := kv.withTimeout(ctx)
	defer cancel()

	txn, err := kv.beginTxnWithTrace("standalone")
	if err != nil {
		return err
	}
	if err := txn.Delete([]byte(key)); err != nil {
		return err
	}
	return commitTiKVTxnWithTrace(ctx, txn, kv.traceOperations, "standalone", kv.commitProtocolFields()...)
}

func (kv *tiKVTxnKV) List(ctx context.Context, prefix, cursor string, limit int) (keys []string, next string, err error) {
	if limit <= 0 {
		limit = 128
	}
	start := time.Now()
	defer func() {
		logTiKVOperation(kv.traceOperations, "list", "standalone", prefix, "", start, err,
			structuredlog.F("result_count", len(keys)),
			structuredlog.F("limit", limit),
			structuredlog.F("cursor_supplied", cursor != ""),
			structuredlog.F("has_next", next != ""),
		)
	}()
	ctx, cancel := kv.withTimeout(ctx)
	defer cancel()

	snapshot := kv.client.GetSnapshot(math.MaxUint64)
	startKey := []byte(prefix)
	if cursor != "" && (prefix == "" || cursor >= prefix) {
		startKey = nextLexicographicKey([]byte(cursor))
	}
	iter, err := snapshot.Iter(startKey, prefixRangeEnd([]byte(prefix)))
	if err != nil {
		return nil, "", err
	}
	defer iter.Close()

	keys = make([]string, 0, limit)
	for iter.Valid() {
		key := string(iter.Key())
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			break
		}
		keys = append(keys, key)
		if limit > 0 && len(keys) >= limit {
			return keys, key, nil
		}
		if err := iter.Next(); err != nil {
			return nil, "", err
		}
	}
	return keys, "", nil
}

func (kv *tiKVTxnKV) RunInTransaction(ctx context.Context, fn func(tx kvReadWriter) error) (err error) {
	start := time.Now()
	var beginDuration time.Duration
	var callbackDuration time.Duration
	var commitDuration time.Duration
	defer func() {
		logTiKVOperation(kv.traceOperations, "transaction", "transaction", "", "", start, err,
			kv.transactionLifecycleFields(beginDuration, callbackDuration, commitDuration)...,
		)
	}()
	ctx, cancel := kv.withTimeout(ctx)
	defer cancel()

	beginStart := time.Now()
	txn, err := kv.beginTxnWithTrace("transaction")
	beginDuration = time.Since(beginStart)
	if err != nil {
		return err
	}
	callbackStart := time.Now()
	if err := fn(&tiKVTxn{txn: txn, traceOperations: kv.traceOperations}); err != nil {
		callbackDuration = time.Since(callbackStart)
		return err
	}
	callbackDuration = time.Since(callbackStart)
	commitStart := time.Now()
	err = commitTiKVTxnWithTrace(ctx, txn, kv.traceOperations, "transaction", kv.commitProtocolFields()...)
	commitDuration = time.Since(commitStart)
	return err
}

func (kv *tiKVTxnKV) Close() error {
	if kv == nil || kv.client == nil {
		return nil
	}
	return kv.client.Close()
}

func (kv *tiKVTxnKV) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline || kv.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, kv.timeout)
}

type tiKVTxnCommitOptionSetter interface {
	SetEnableAsyncCommit(bool)
	SetEnable1PC(bool)
}

func (kv *tiKVTxnKV) configureTxnCommitOptions(txn tiKVTxnCommitOptionSetter) {
	if kv == nil || txn == nil {
		return
	}
	txn.SetEnableAsyncCommit(kv.enableAsyncCommit)
	txn.SetEnable1PC(kv.enableOnePhaseCommit)
}

func (kv *tiKVTxnKV) beginTxnWithTrace(scope string) (txn *txnkv.KVTxn, err error) {
	start := time.Now()
	defer func() {
		logTiKVOperation(kv.traceOperations, "txn_begin", scope, "", "", start, err, kv.commitProtocolFields()...)
	}()
	txn, err = kv.client.Begin()
	if err != nil {
		return nil, err
	}
	kv.configureTxnCommitOptions(txn)
	return txn, nil
}

func (kv *tiKVTxnKV) commitProtocolFields() []structuredlog.Field {
	if kv == nil {
		return nil
	}
	return []structuredlog.Field{
		structuredlog.F("async_commit_enabled", kv.enableAsyncCommit),
		structuredlog.F("one_phase_commit_enabled", kv.enableOnePhaseCommit),
	}
}

func (kv *tiKVTxnKV) transactionLifecycleFields(beginDuration, callbackDuration, commitDuration time.Duration) []structuredlog.Field {
	fields := kv.commitProtocolFields()
	fields = append(fields,
		structuredlog.F("txn_begin_duration_ms", beginDuration.Milliseconds()),
		structuredlog.F("txn_callback_duration_ms", callbackDuration.Milliseconds()),
		structuredlog.F("txn_commit_duration_ms", commitDuration.Milliseconds()),
	)
	return fields
}

func isTiKVWriteConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "write conflict") || strings.Contains(msg, "optimistic")
}

func commitTiKVTxn(ctx context.Context, txn *txnkv.KVTxn) error {
	return commitTiKVTxnWithTrace(ctx, txn, false, "")
}

func commitTiKVTxnWithTrace(ctx context.Context, txn *txnkv.KVTxn, traceOperations bool, scope string, extra ...structuredlog.Field) error {
	start := time.Now()
	if err := txn.Commit(ctx); err != nil {
		if isTiKVWriteConflict(err) {
			fields := append([]structuredlog.Field{}, extra...)
			fields = append(fields, structuredlog.F("cas_conflict", true))
			logTiKVOperation(traceOperations, "commit", scope, "", "cas_conflict", start, ErrCASConflict, fields...)
			return ErrCASConflict
		}
		logTiKVOperation(traceOperations, "commit", scope, "", "error", start, err, extra...)
		return err
	}
	logTiKVOperation(traceOperations, "commit", scope, "", "ok", start, nil, extra...)
	return nil
}

type tiKVTxn struct {
	txn             *txnkv.KVTxn
	traceOperations bool
}

const tiKVTxnBatchGetPointFallbackMaxUniqueKeys = 2

func (tx *tiKVTxn) Get(ctx context.Context, key string) (out []byte, found bool, err error) {
	start := time.Now()
	defer func() {
		outcome := "ok"
		if err != nil {
			outcome = "error"
		} else if !found {
			outcome = "not_found"
		}
		logTiKVOperation(tx.traceOperations, "txn_get", "transaction", key, outcome, start, err,
			structuredlog.F("found", found),
		)
	}()
	value, err := tx.txn.Get(ctx, []byte(key))
	if tikverr.IsErrNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return append([]byte(nil), value...), true, nil
}

func (tx *tiKVTxn) BatchGet(ctx context.Context, keys []string) (out map[string][]byte, err error) {
	start := time.Now()
	foundCount := 0
	uniqueKeyCount := 0
	batchGetMode := "batch_get"
	defer func() {
		outcome := "ok"
		if err != nil {
			outcome = "error"
		}
		logTiKVOperation(tx.traceOperations, "txn_batch_get", "transaction", "", outcome, start, err,
			structuredlog.F("key_count", len(keys)),
			structuredlog.F("unique_key_count", uniqueKeyCount),
			structuredlog.F("found_count", foundCount),
			structuredlog.F("batch_get_mode", batchGetMode),
		)
	}()
	out = make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		batchGetMode = "empty"
		return out, nil
	}
	uniqueKeys, rawKeys := uniqueTiKVBatchGetKeys(keys)
	uniqueKeyCount = len(uniqueKeys)
	if useTiKVTxnBatchGetPointFallback(uniqueKeyCount) {
		batchGetMode = "point_get_fallback"
		for _, key := range uniqueKeys {
			value, getErr := tx.txn.Get(ctx, []byte(key))
			if tikverr.IsErrNotFound(getErr) {
				continue
			}
			if getErr != nil {
				return nil, getErr
			}
			foundCount++
			out[key] = append([]byte(nil), value...)
		}
		return out, nil
	}
	values, err := tx.txn.BatchGet(ctx, rawKeys)
	if err != nil {
		return nil, err
	}
	for key, value := range values {
		foundCount++
		out[key] = append([]byte(nil), value...)
	}
	return out, nil
}

func uniqueTiKVBatchGetKeys(keys []string) ([]string, [][]byte) {
	uniqueKeys := make([]string, 0, len(keys))
	rawKeys := make([][]byte, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		uniqueKeys = append(uniqueKeys, key)
		rawKeys = append(rawKeys, []byte(key))
	}
	return uniqueKeys, rawKeys
}

func useTiKVTxnBatchGetPointFallback(uniqueKeyCount int) bool {
	return uniqueKeyCount > 0 && uniqueKeyCount <= tiKVTxnBatchGetPointFallbackMaxUniqueKeys
}

func (tx *tiKVTxn) Set(_ context.Context, key string, value []byte) error {
	return tx.txn.Set([]byte(key), append([]byte(nil), value...))
}

func (tx *tiKVTxn) Delete(_ context.Context, key string) error {
	return tx.txn.Delete([]byte(key))
}

func logTiKVOperation(traceOperations bool, operation, scope, key, outcome string, start time.Time, err error, extra ...structuredlog.Field) {
	if !traceOperations {
		return
	}
	if scope == "" {
		scope = "standalone"
	}
	if outcome == "" {
		if err != nil {
			outcome = "error"
		} else {
			outcome = "ok"
		}
	}
	fields := []structuredlog.Field{
		structuredlog.F("backend", "tikv"),
		structuredlog.F("operation", operation),
		structuredlog.F("scope", scope),
		structuredlog.F("outcome", outcome),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
	}
	if keyClass := classifyTiKVMetadataKey(key); keyClass != "" {
		fields = append(fields, structuredlog.F("key_class", keyClass))
	}
	fields = append(fields, extra...)
	if err != nil {
		structuredlog.Error("sbs.metadata", "tikv_operation_completed", err, fields...)
		return
	}
	structuredlog.Info("sbs.metadata", "tikv_operation_completed", fields...)
}

func classifyTiKVMetadataKey(key string) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	if key == "" {
		return ""
	}
	if strings.HasPrefix(key, "keyspaces/") {
		parts := strings.SplitN(key, "/", 3)
		if len(parts) == 3 {
			key = parts[2]
		}
	}
	parts := strings.Split(key, "/")
	if len(parts) >= 2 && parts[0] == "sbs" && parts[1] == "cluster" {
		parts = parts[2:]
	}
	if len(parts) == 0 {
		return "unknown"
	}
	switch parts[0] {
	case "admin":
		if len(parts) >= 2 && parts[1] == "volumes" {
			return "volume_spec"
		}
	case "bootstrap":
		return "bootstrap"
	case "clones":
		if len(parts) >= 3 && parts[2] == "delta" {
			return "clone_delta_allocation_page"
		}
		return "clone"
	case "ec":
		return "ec_profile"
	case "nodes":
		if len(parts) >= 3 && parts[2] == "health" {
			return "node_health"
		}
		return "node_membership"
	case "snapshots":
		if len(parts) >= 3 && parts[2] == "allocation" {
			return "snapshot_allocation_page"
		}
		if len(parts) >= 3 && parts[2] == "clone_idem" {
			return "clone_idempotency"
		}
		if len(parts) >= 3 && parts[2] == "clones" {
			return "snapshot_clone_index"
		}
		return "snapshot"
	case "topology":
		return "topology"
	case "volumes":
		if len(parts) < 3 {
			return "volume"
		}
		switch parts[2] {
		case "allocation":
			return "allocation_page"
		case "clones":
			return "clone_source_index"
		case "ec":
			return "ec_stripe"
		case "extents":
			return "extent_mapping"
		case "idem":
			return "idempotency"
		case "meta":
			if len(parts) >= 4 && parts[3] == "next_chunk_id" {
				return "chunk_id_sequence"
			}
			if len(parts) >= 4 && parts[3] == "state" {
				return "volume_state"
			}
			return "volume_meta"
		case "operations":
			return "mutation_operation"
		case "physical_objects":
			return "physical_object"
		case "placements":
			return "placement_transition"
		case "replicasets":
			return "replica_set"
		case "snapshot_idem":
			return "snapshot_idempotency"
		case "snapshots":
			return "snapshot_source_index"
		case "write_state":
			return "range_local_write_state"
		}
	}
	return parts[0]
}

func parseTxnKVAPIVersion(version tikvopts.APIVersion) kvrpcpb.APIVersion {
	switch version {
	case tikvopts.APIVersionV2:
		return kvrpcpb.APIVersion_V2
	case "", tikvopts.APIVersionV1:
		return kvrpcpb.APIVersion_V1
	default:
		return kvrpcpb.APIVersion_V1
	}
}

func prefixRangeEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func nextLexicographicKey(key []byte) []byte {
	next := make([]byte, len(key)+1)
	copy(next, key)
	return next
}
