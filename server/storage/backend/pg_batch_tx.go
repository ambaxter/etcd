package backend

import (
	"context"
	"sync"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/stephenafamo/bob/dialect/psql/dialect"
	"github.com/tidwall/btree"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
)

type UnsafePgReadWriter interface {
	UnsafePgReader
	UnsafePgWriter
}

type UnsafePgWriter interface {
	UnsafeKvWriter
}

type pgBucketBatch struct {
	bucket  Bucket
	updates map[string][]byte
	deletes map[string]struct{}
	cBucket bool
	dBucket bool
}

type PgKvLogEntry struct {
	RevMain   int64
	RevSub    int64
	RevCreate int64
	Lease     zeronull.Int8
	Version   int64
	Key       []byte
	Value     []byte
}

func (e *PgKvLogEntry) toKeyValue() mvccpb.KeyValue {
	return mvccpb.KeyValue{
		Key:            e.Key,
		CreateRevision: e.RevCreate,
		ModRevision:    e.RevMain,
		Version:        e.Version,
		Lease:          int64(e.Lease),
		Value:          e.Value,
	}
}

type PgKvLogEntrySoa struct {
	RevMains   []int64
	RevSubs    []int64
	RevCreates []int64
	Leases     []zeronull.Int8
	Version    []int64
	Keys       [][]byte
	Values     [][]byte
}

func newPgKvLogEntrySoa() *PgKvLogEntrySoa {
	p := &PgKvLogEntrySoa{}
	p.resetStore()
	return p
}

func (a *PgKvLogEntrySoa) resetStore() {
	a.RevMains = make([]int64, 0, 64)
	a.RevSubs = make([]int64, 0, 64)
	a.RevCreates = make([]int64, 0, 64)
	a.Leases = make([]zeronull.Int8, 0, 64)
	a.Version = make([]int64, 0, 64)
	a.Keys = make([][]byte, 0, 64)
	a.Values = make([][]byte, 0, 64)
}

func (a *PgKvLogEntrySoa) clear() {
	clear(a.RevMains)
	a.RevMains = a.RevMains[:0]
	clear(a.RevSubs)
	a.RevSubs = a.RevSubs[:0]
	clear(a.RevCreates)
	a.RevCreates = a.RevCreates[:0]
	clear(a.Leases)
	a.Leases = a.Leases[:0]
	clear(a.Version)
	a.Version = a.Version[:0]
	clear(a.Keys)
	a.Keys = a.Keys[:0]
	clear(a.Values)
	a.Values = a.Values[:0]
}

func (a *PgKvLogEntrySoa) pushEntry(e *PgKvLogEntry) {
	a.push(e.RevMain, e.RevSub, e.RevCreate, e.Lease, e.Version, e.Key, e.Value)
}

func (a *PgKvLogEntrySoa) push(revMain, revSub, revCreate int64, lease zeronull.Int8, version int64, key, value []byte) {
	a.RevMains = append(a.RevMains, revMain)
	a.RevSubs = append(a.RevSubs, revSub)
	a.RevCreates = append(a.RevCreates, revCreate)
	a.Leases = append(a.Leases, lease)
	a.Version = append(a.Version, version)
	a.Keys = append(a.Keys, key)
	a.Values = append(a.Values, value)
}

type pgKvBatch struct {
	lg                  *zap.Logger
	kvLogs              *btree.Map[int64, *btree.Map[int64, *PgKvLogEntry]]
	batchKvDeltaEntries *PgKvLogEntrySoa
}

func newPgKvBatch(lg *zap.Logger) *pgKvBatch {
	return &pgKvBatch{
		lg:                  lg,
		kvLogs:              btree.NewMap[int64, *btree.Map[int64, *PgKvLogEntry]](32),
		batchKvDeltaEntries: newPgKvLogEntrySoa(),
	}
}

func (b *pgKvBatch) hasEntries() bool {
	return b.kvLogs.Len() != 0
}

func (b *pgKvBatch) getKvTree(revMain int64) *btree.Map[int64, *PgKvLogEntry] {
	tree, ok := b.kvLogs.GetMut(revMain)
	if !ok {
		tree = new(btree.Map[int64, *PgKvLogEntry])
		b.kvLogs.Set(revMain, tree)
	}
	return tree
}

func (b *pgKvBatch) kvPutKey(entry *PgKvLogEntry) {
	subTree := b.getKvTree(entry.RevMain)
	subTree.Set(entry.RevSub, entry)
}

func (b *pgKvBatch) kvDeleteKey(revMain, revSub int64, key []byte) {
	entry := &PgKvLogEntry{
		RevMain: revMain,
		RevSub:  revSub,
		Key:     key,
	}
	subTree := b.getKvTree(revMain)
	subTree.Set(revSub, entry)
}

func (b *pgKvBatch) queueKvUpdate(batch *pgx.Batch, query pgFn[*dialect.InsertQuery]) {
	if !b.hasEntries() {
		return
	}
	b.batchKvDeltaEntries.clear()
	iMain := b.kvLogs.Iter()
	for iMain.Next() {
		subTree := iMain.Value()
		iSub := subTree.Iter()
		for iSub.Next() {
			b.batchKvDeltaEntries.pushEntry(iSub.Value())
		}
	}
	q := batch.Queue(query.Fn(),
		b.batchKvDeltaEntries.RevMains,
		b.batchKvDeltaEntries.RevSubs,
		b.batchKvDeltaEntries.RevCreates,
		b.batchKvDeltaEntries.Leases,
		b.batchKvDeltaEntries.Version,
		b.batchKvDeltaEntries.Keys,
		b.batchKvDeltaEntries.Values,
	)
	if b.lg.Level() == zap.DebugLevel {
		q.Exec(func(ct pgconn.CommandTag) error {
			b.lg.Info("Upsert kv_log", zap.Int("entries", len(b.batchKvDeltaEntries.RevMains)))
			return nil
		})
	}
}

func (b *pgKvBatch) clearKvUpdate() {
	b.kvLogs.Clear()
}

func (b *pgKvBatch) clearKvCommit() {
	b.kvLogs.Clear()
	b.batchKvDeltaEntries.resetStore()
}

type pgDbBatch struct {
	lg          *zap.Logger
	bucketBatch map[string]*pgBucketBatch
	kvType      PgKvType
	len         int
}

func newPgDbBatch(lg *zap.Logger, kvType PgKvType) *pgDbBatch {
	return &pgDbBatch{
		lg:          lg,
		bucketBatch: make(map[string]*pgBucketBatch, 11),
		kvType:      kvType,
	}
}

func (b *pgDbBatch) hasEntries() bool {
	return b.len != 0
}

func (b *pgDbBatch) getBucketBatch(bucket Bucket, assertNotDeleted bool) *pgBucketBatch {
	name := unsafe.String(unsafe.SliceData(bucket.Name()), len(bucket.Name()))
	batch, ok := b.bucketBatch[name]
	if !ok {
		batch = &pgBucketBatch{
			bucket:  bucket,
			updates: make(map[string][]byte, 10),
			deletes: make(map[string]struct{}, 10),
		}
		b.bucketBatch[name] = batch
	}
	if assertNotDeleted && batch.dBucket {
		panic("batch deleted bucket: " + bucket.String())
	}
	return batch
}

func (b *pgDbBatch) createBucket(bucket Bucket) {
	batch := b.getBucketBatch(bucket, true)
	batch.cBucket = true
	b.len += 1
}

func (b *pgDbBatch) put(bucket Bucket, key, value []byte) {
	batch := b.getBucketBatch(bucket, true)
	k := unsafe.String(unsafe.SliceData(key), len(key))
	batch.cBucket = true
	delete(batch.deletes, k)
	batch.updates[k] = value
	b.len += 1
}

func (b *pgDbBatch) delete(bucket Bucket, key []byte) {
	batch := b.getBucketBatch(bucket, true)
	k := unsafe.String(unsafe.SliceData(key), len(key))
	delete(batch.updates, k)
	batch.deletes[k] = struct{}{}
	b.len += 1
}

func (b *pgDbBatch) deleteBucket(bucket Bucket) {
	batch := b.getBucketBatch(bucket, false)
	clear(batch.deletes)
	clear(batch.updates)
	batch.dBucket = true
	b.len += 1
}

func (b *pgDbBatch) queueDbUpdates(batch *pgx.Batch) {
	if !b.hasEntries() {
		return
	}
	for _, delta := range b.bucketBatch {
		if delta.cBucket {
			var q *pgx.QueuedQuery
			switch b.kvType {
			case KvBucketKeys:
				if !delta.bucket.IsKeys() {
					q = batch.Queue(BUCKETS_CREATE_BUCKET.Fn(), delta.bucket.Name())
				}
			default:
				q = batch.Queue(BUCKETS_CREATE_BUCKET.Fn(), delta.bucket.Name())
			}
			if b.lg.Level() == zap.DebugLevel && q != nil {
				q.Exec(func(ct pgconn.CommandTag) error {
					b.lg.Debug("Create bucket", zap.String("bucket", delta.bucket.String()), zap.Int64("entries", ct.RowsAffected()))
					return nil
				})
			}
		}
		if len(delta.updates) > 0 {
			var keys, values [][]byte
			for k, v := range delta.updates {
				keys = append(keys, []byte(k))
				values = append(values, v)
			}
			var q *pgx.QueuedQuery
			switch b.kvType {
			case KvBucketKeys:
				if delta.bucket.IsKeys() {
					q = batch.Queue(KEYS_BATCH_UPSERT_KEY.Fn(), keys, values)
				} else {
					q = batch.Queue(BUCKETS_BATCH_UPSERT_KEY.Fn(), delta.bucket.Name(), keys, values)
				}
			default:
				q = batch.Queue(BUCKETS_BATCH_UPSERT_KEY.Fn(), delta.bucket.Name(), keys, values)
			}
			if b.lg.Level() == zap.DebugLevel {
				q.Exec(func(ct pgconn.CommandTag) error {
					b.lg.Info("Upsert into bucket", zap.String("bucket", delta.bucket.String()), zap.Int("entries", len(values)))
					return nil
				})
			}
		}
		if len(delta.deletes) > 0 {
			var keys [][]byte
			for k := range delta.deletes {
				keys = append(keys, []byte(k))
			}
			var q *pgx.QueuedQuery
			switch b.kvType {
			case KvBucketKeys:
				if delta.bucket.IsKeys() {
					q = batch.Queue(KEYS_BATCH_DELETE_KEY.Fn(), keys)
				} else {
					q = batch.Queue(BUCKETS_BATCH_DELETE_KEY.Fn(), delta.bucket.Name(), keys)
				}
			default:
				q = batch.Queue(BUCKETS_BATCH_DELETE_KEY.Fn(), delta.bucket.Name(), keys)
			}
			if b.lg.Level() == zap.DebugLevel {
				q.Exec(func(ct pgconn.CommandTag) error {
					b.lg.Debug("Delete from bucket", zap.String("bucket", delta.bucket.String()), zap.Int64("entries", ct.RowsAffected()))
					return nil
				})
			}
		}
		if delta.dBucket {
			var q *pgx.QueuedQuery
			switch b.kvType {
			case KvBucketKeys:
				if delta.bucket.IsKeys() {
					q = batch.Queue(KEYS_BATCH_TRUNCATE.Fn())
				} else {
					q = batch.Queue(BUCKETS_DELETE_BUCKET.Fn(), delta.bucket.Name())
				}
			default:
				q = batch.Queue(BUCKETS_DELETE_BUCKET.Fn(), delta.bucket.Name())
			}
			if b.lg.Level() == zap.DebugLevel {
				q.Exec(func(ct pgconn.CommandTag) error {
					b.lg.Debug("Delete bucket", zap.String("bucket", delta.bucket.String()), zap.Int64("entries", ct.RowsAffected()))
					return nil
				})
			}
		}
	}
}

func (b *pgDbBatch) clearDbUpdate() {
	for _, delta := range b.bucketBatch {
		if delta.cBucket {
			delta.cBucket = false
		}
		if len(delta.updates) > 0 {
			clear(delta.updates)
		}
		if len(delta.deletes) > 0 {
			clear(delta.deletes)
		}
		if delta.dBucket {
			delta.dBucket = false
		}
	}
	b.len = 0
}

type pgBatchTx struct {
	pgSharedTx
	compactLock sync.Mutex
	tx          pgx.Tx
	txBatch     *pgx.Batch
	backend     *pgBackend
	subDbBatch  *pgDbBatch
	subKvBatch  *pgKvBatch
}

func (t *pgBatchTx) updateDbTx() {
	t.subDbBatch.queueDbUpdates(t.txBatch)
	switch t.kvType {
	case KvLbrNowNorm:
		t.subKvBatch.queueKvUpdate(t.txBatch, KV_LBR_NOW_NORM_UPSERT)
	case KvLbrNowNifs:
		t.subKvBatch.queueKvUpdate(t.txBatch, KV_LBR_NOW_NIFS_UPSERT)
	case KvLbrKidNowNorm:
		t.subKvBatch.queueKvUpdate(t.txBatch, KV_LBR_KID_NOW_NORM_UPSERT)
	case KvLbrKidNowNifs:
		t.subKvBatch.queueKvUpdate(t.txBatch, KV_LBR_KID_NOW_NIFS_UPSERT)
	}
	if t.txBatch.Len() > 0 {
		results := t.tx.SendBatch(context.Background(), t.txBatch)
		err := results.Close()
		if err != nil {
			t.lg.Fatal("error sending sub-batch", zap.Error(err))
		}
	}
	t.subDbBatch.clearDbUpdate()
	t.subKvBatch.clearKvUpdate()
	t.txBatch = &pgx.Batch{}
}

func (t *pgBatchTx) unsafeCreateBucket(bucket Bucket) {
	t.assertNotKvAware(bucket)
	t.subDbBatch.createBucket(bucket)
}

func (t *pgBatchTx) unsafeDeleteBucket(bucket Bucket) {
	t.assertNotKvAware(bucket)
	t.subDbBatch.deleteBucket(bucket)
}

func (t *pgBatchTx) unsafePutShared(bucketType Bucket, key []byte, value []byte) {
	t.assertNotKvAware(bucketType)
	t.subDbBatch.put(bucketType, key, value)
}

func (t *pgBatchTx) unsafeExactKeys(bucket Bucket, inDb [][]byte, vals map[string][]byte) map[string][]byte {
	t.assertNotKvAware(bucket)
	if bucket.IsKeys() && t.kvType == KvBucketKeys {
		return t.unsafeExactKeysShared(KEYS_EXACT, vals, inDb)
	} else {
		return t.unsafeExactKeysShared(BUCKETS_EXACT, vals, bucket.Name(), inDb)
	}
}

func (t *pgBatchTx) unsafeExactKeysShared(query pgFn[*dialect.SelectQuery], vals map[string][]byte, vars ...any) map[string][]byte {
	t.updateDbTx()
	rows, err := t.tx.Query(context.Background(), query.Fn(), vars...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	vals, err = exactKeys(rows, vals)
	if err != nil {
		panic(err)
	}
	return vals
}

func (t *pgBatchTx) unsafeRange(bucketType Bucket, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	t.assertNotKvAware(bucketType)
	if bucketType.IsKeys() && t.kvType == KvBucketKeys {
		return t.unsafeRangeShared(KEYS_RANGE, key, endKey, zeronull.Int8(limit))
	} else {
		return t.unsafeRangeShared(BUCKETS_RANGE, bucketType.Name(), key, endKey, zeronull.Int8(limit))
	}
}

func (t *pgBatchTx) unsafeRangeShared(query pgFn[*dialect.SelectQuery], vars ...any) (keys [][]byte, vals [][]byte) {
	t.updateDbTx()
	rows, err := t.tx.Query(context.Background(), query.Fn(), vars...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	return rowsToRange(rows)
}

func (t *pgBatchTx) unsafeDelete(bucket Bucket, key []byte) {
	t.assertNotKvAware(bucket)
	t.subDbBatch.delete(bucket, key)
}

func (t *pgBatchTx) unsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	t.assertNotKvAware(bucket)
	if bucket.IsKeys() && t.kvType == KvBucketKeys {
		return t.unsafeForEachShared(KEYS_SCAN, visitor)
	} else {
		return t.unsafeForEachShared(BUCKETS_SCAN, visitor, bucket.Name())
	}
}

func (t *pgBatchTx) unsafeForEachShared(query pgFn[*dialect.SelectQuery], visitor func(k, v []byte) error, vars ...any) error {
	t.updateDbTx()
	rows, err := t.tx.Query(context.Background(), query.Fn(), vars...)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = forEachRow(rows, visitor)
	return err
}

func (t *pgBatchTx) unsafeKvRangeEntries(key, endKey []byte, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
	t.assertKvAware()
	switch t.kvType {
	case KvLbrNowNorm:
		return t.unsafeKvRangeEntriesShared(KV_LBR_NOW_NORM_RANGE_ENTRY, key, endKey, limit, ro)
	case KvLbrNowNifs:
		return t.unsafeKvRangeEntriesShared(KV_LBR_NOW_NIFS_RANGE_ENTRY, key, endKey, limit, ro)
	case KvLbrKidNowNorm:
		return t.unsafeKvRangeEntriesShared(KV_LBR_KID_NOW_NORM_RANGE_ENTRY, key, endKey, limit, ro)
	case KvLbrKidNowNifs:
		return t.unsafeKvRangeEntriesShared(KV_LBR_KID_NOW_NIFS_RANGE_ENTRY, key, endKey, limit, ro)
	default:
		t.lg.Panic("Unexpected kvType", zap.String("kvType", t.kvType.String()))
	}
	// we panic, but go doesn't realize that
	return nil
}

func (t *pgBatchTx) unsafeKvRangeEntriesShared(query pgFn[*dialect.SelectQuery], key, endKey []byte, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
	t.updateDbTx()
	minCreateRev, maxCreateRev, minModRev, maxModRev := ro.toNullable()
	nLimit := zeronull.Int8(limit)
	rows, err := t.tx.Query(context.Background(), query.Fn(), key, endKey, nLimit, minCreateRev, maxCreateRev, minModRev, maxModRev)
	if err != nil {
		t.lg.Panic("Error reading entries", zap.Error(err))
	}
	defer rows.Close()
	return rowsToEntries(t.lg, rows)
}

func (t *pgBatchTx) unsafeKvLogRangeEntries(key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
	t.assertKvAware()
	switch t.kvType {
	case KvLbrNowNorm, KvLbrNowNifs:
		return t.unsafeKvLogRangeEntriesShared(KV_LBR_RANGE_ENTRY, key, endKey, latestRev, limit, ro)
	case KvLbrKidNowNorm, KvLbrKidNowNifs:
		return t.unsafeKvLogRangeEntriesShared(KV_LBR_KID_RANGE_ENTRY, key, endKey, latestRev, limit, ro)
	default:
		t.lg.Panic("Unexpected kvType", zap.String("kvType", t.kvType.String()))
	}
	// we panic, but go doesn't realize that
	return nil
}

func (t *pgBatchTx) unsafeKvLogRangeEntriesShared(query pgFn[*dialect.SelectQuery], key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
	t.updateDbTx()
	minCreateRev, maxCreateRev, minModRev, maxModRev := ro.toNullable()
	nLatestRev, nLimit := zeronull.Int8(latestRev), zeronull.Int8(limit)
	rows, err := t.tx.Query(context.Background(), query.Fn(), key, endKey, nLatestRev, nLimit, minCreateRev, maxCreateRev, minModRev, maxModRev)
	if err != nil {
		t.lg.Panic("Error reading entries", zap.Error(err))
	}
	defer rows.Close()
	return rowsToEntries(t.lg, rows)
}

func (t *pgBatchTx) unsafeKvRangeKeys(key, endKey []byte, limit int64, ro KvRangeOptions) [][]byte {
	t.assertKvAware()
	switch t.kvType {
	case KvLbrNowNorm:
		return t.unsafeKvRangeKeysShared(KV_LBR_NOW_NORM_RANGE_KEY, key, endKey, limit, ro)
	case KvLbrNowNifs:
		return t.unsafeKvRangeKeysShared(KV_LBR_NOW_NIFS_RANGE_KEY, key, endKey, limit, ro)
	case KvLbrKidNowNorm:
		return t.unsafeKvRangeKeysShared(KV_LBR_KID_NOW_NORM_RANGE_KEY, key, endKey, limit, ro)
	case KvLbrKidNowNifs:
		return t.unsafeKvRangeKeysShared(KV_LBR_KID_NOW_NIFS_RANGE_KEY, key, endKey, limit, ro)
	default:
		t.lg.Panic("Unexpected kvType", zap.String("kvType", t.kvType.String()))
	}
	// we panic, but go doesn't realize that
	return nil
}

func (t *pgBatchTx) unsafeKvRangeKeysShared(query pgFn[*dialect.SelectQuery], key, endKey []byte, limit int64, ro KvRangeOptions) [][]byte {
	t.updateDbTx()
	minCreateRev, maxCreateRev, minModRev, maxModRev := ro.toNullable()
	nLimit := zeronull.Int8(limit)
	rows, err := t.tx.Query(context.Background(), query.Fn(), key, endKey, nLimit, minCreateRev, maxCreateRev, minModRev, maxModRev)
	if err != nil {
		t.lg.Panic("Error reading entries", zap.Error(err))
	}
	defer rows.Close()
	return rowsToKeys(t.lg, rows)
}

func (t *pgBatchTx) unsafeKvLogRangeKeys(key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) [][]byte {
	t.assertKvAware()
	switch t.kvType {
	case KvLbrNowNorm, KvLbrNowNifs:
		return t.unsafeKvLogRangeKeysShared(KV_LBR_RANGE_KEY, key, endKey, latestRev, limit, ro)
	case KvLbrKidNowNorm, KvLbrKidNowNifs:
		return t.unsafeKvLogRangeKeysShared(KV_LBR_KID_RANGE_KEY, key, endKey, latestRev, limit, ro)
	default:
		t.lg.Panic("Unexpected kvType", zap.String("kvType", t.kvType.String()))
	}
	// we panic, but go doesn't realize that
	return nil
}

func (t *pgBatchTx) unsafeKvLogRangeKeysShared(query pgFn[*dialect.SelectQuery], key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) [][]byte {
	t.updateDbTx()
	minCreateRev, maxCreateRev, minModRev, maxModRev := ro.toNullable()
	nLatestRev, nLimit := zeronull.Int8(latestRev), zeronull.Int8(limit)
	rows, err := t.tx.Query(context.Background(), query.Fn(), key, endKey, nLatestRev, nLimit, minCreateRev, maxCreateRev, minModRev, maxModRev)
	if err != nil {
		t.lg.Panic("Error reading keys", zap.Error(err))
	}
	defer rows.Close()
	return rowsToKeys(t.lg, rows)
}

func (t *pgBatchTx) unsafeKvPutKey(entry *PgKvLogEntry) {
	t.assertKvAware()
	t.subKvBatch.kvPutKey(entry)
}

func (t *pgBatchTx) unsafeKvDeleteKey(revMain, revSub int64, key []byte) {
	t.assertKvAware()
	t.subKvBatch.kvDeleteKey(revMain, revSub, key)
}

func (t *pgBatchTx) unsafeKvLogCompact(compactMainRev int64, visitor func(entry mvccpb.KeyValue) error) (int64, error) {
	t.assertKvAware()
	switch t.kvType {
	case KvLbrNowNorm:
		return t.unsafeKvLogCompactShared(KV_LBR_NOW_NORM_COMPACT, compactMainRev, visitor)
	case KvLbrNowNifs:
		return t.unsafeKvLogCompactShared(KV_LBR_NOW_NIFS_COMPACT, compactMainRev, visitor)
	case KvLbrKidNowNorm:
		return t.unsafeKvLogCompactShared(KV_LBR_KID_NOW_NORM_COMPACT, compactMainRev, visitor)
	case KvLbrKidNowNifs:
		return t.unsafeKvLogCompactShared(KV_LBR_KID_NOW_NIFS_COMPACT, compactMainRev, visitor)
	default:
		t.lg.Panic("Unexpected kvType", zap.String("kvType", t.kvType.String()))
	}
	// we panic, but go doesn't realize that
	return 0, nil
}

func (t *pgBatchTx) unsafeKvLogCompactShared(query pgFn[*dialect.SelectQuery], compactMainRev int64, visitor func(entry mvccpb.KeyValue) error) (int64, error) {
	nCompactMainRev := zeronull.Int8(compactMainRev)
	rows, err := t.backend.writePool.Query(context.Background(), query.Fn(), nCompactMainRev)
	if err != nil {
		t.lg.Panic("compact", zap.Error(err))
		return 0, err
	}
	defer rows.Close()

	count := int64(0)
	cPtr := &count
	for rows.Next() {
		var revCreate, revMod, version int64
		var lease zeronull.Int8
		var key, value []byte
		err := rows.Scan(&revCreate, &revMod, &lease, &version, &key, &value)
		kv := mvccpb.KeyValue{
			Key:            key,
			CreateRevision: revCreate,
			ModRevision:    revMod,
			Lease:          int64(lease),
			Version:        version,
			Value:          value,
		}
		if err != nil {
			return 0, err
		}
		err = visitor(kv)
		if err != nil {
			return 0, err
		}
		(*cPtr) += 1
	}
	return count, nil
}

func (t *pgBatchTx) unsafeKvLogForEachByRev(latestRev int64, visitor func(entry mvccpb.KeyValue) error) error {
	t.assertKvAware()
	switch t.kvType {
	case KvLbrNowNorm, KvLbrNowNifs:
		return t.unsafeKvLogForEachByRevShared(KV_LBR_RANGE_KEY, latestRev, visitor)
	case KvLbrKidNowNorm, KvLbrKidNowNifs:
		return t.unsafeKvLogForEachByRevShared(KV_LBR_KID_RANGE_KEY, latestRev, visitor)
	default:
		t.lg.Panic("Unexpected kvType", zap.String("kvType", t.kvType.String()))
	}
	// we panic, but go doesn't realize that
	return nil
}

func (t *pgBatchTx) unsafeKvLogForEachByRevShared(query pgFn[*dialect.SelectQuery], latestRev int64, visitor func(entry mvccpb.KeyValue) error) error {
	t.updateDbTx()
	nLatestRev := zeronull.Int8(latestRev)
	rows, err := t.tx.Query(context.Background(), query.Fn(), nLatestRev)
	if err != nil {
		t.lg.Panic("Error reading log", zap.Error(err))
	}
	defer rows.Close()
	for rows.Next() {
		var revCreate, revMod, version int64
		var lease zeronull.Int8
		var key, value []byte
		err := rows.Scan(&revCreate, &revMod, &lease, &version, &key, &value)
		kv := mvccpb.KeyValue{
			Key:            key,
			CreateRevision: revCreate,
			ModRevision:    revMod,
			Lease:          int64(lease),
			Version:        version,
			Value:          value,
		}
		if err != nil {
			return err
		}
		err = visitor(kv)
		if err != nil {
			return err
		}
	}
	return nil
}
