package backend

import (
	"context"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stephenafamo/bob/dialect/psql/dialect"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
)

type pgSharedTx struct {
	lg     *zap.Logger
	kvType PgKvType
}

func (t *pgSharedTx) IsKvAware() bool {
	switch t.kvType {
	case KvBucket, KvBucketKeys:
		return false
	default:
		return true
	}
}

func (t *pgSharedTx) assertNotKvAware(bucket Bucket) {
	switch t.kvType {
	case KvLbrNowNorm, KvLbrNowNifs:
		if bucket.IsKeys() {
			t.lg.Panic("Attempting to use bucket functions while kv aware!", zap.String("kvType", t.kvType.String()))
		}
	}
}

func (t *pgSharedTx) assertKvAware() {
	switch t.kvType {
	case KvBucket, KvBucketKeys:
		t.lg.Panic("Attempting to use kv aware functions!", zap.String("kvType", t.kvType.String()))

	}
}

type pgReadTx struct {
	pgSharedTx
	readPool *pgxpool.Pool
	kvBuffer PgKvBuffer[*PgKvLogEntry]
}

func (t *pgReadTx) unsafeExactKeys(bucket Bucket, inDb [][]byte, vals map[string][]byte) map[string][]byte {
	t.assertNotKvAware(bucket)
	if bucket.IsKeys() && t.kvType == KvBucketKeys {
		return t.unsafeExactKeysShared(KEYS_EXACT, vals, inDb)
	} else {
		return t.unsafeExactKeysShared(BUCKETS_EXACT, vals, bucket.Name(), inDb)
	}
}

func (t *pgReadTx) unsafeExactKeysShared(query pgFn[*dialect.SelectQuery], vals map[string][]byte, vars ...any) map[string][]byte {
	rows, err := t.readPool.Query(context.Background(), query.Fn(), vars...)
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

func exactKeys(rows pgx.Rows, vals map[string][]byte) (map[string][]byte, error) {
	for rows.Next() {
		var rKey, rVal []byte
		err := rows.Scan(&rKey, &rVal)
		if err != nil {
			return nil, err
		}
		strKey := unsafe.String(unsafe.SliceData(rKey), len(rKey))
		vals[strKey] = rVal
	}
	return vals, nil
}

func forEachRow(rows pgx.Rows, visitor func(k, v []byte) error) error {
	for rows.Next() {
		var key, value []byte
		err := rows.Scan(&key, &value)
		if err != nil {
			return err
		}
		err = visitor(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *pgReadTx) unsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	t.assertNotKvAware(bucket)
	if bucket.IsKeys() && t.kvType == KvBucketKeys {
		return t.unsafeForEachShared(KEYS_SCAN, visitor)
	} else {
		return t.unsafeForEachShared(BUCKETS_SCAN, visitor, bucket.Name())
	}
}

func (t *pgReadTx) unsafeForEachShared(query pgFn[*dialect.SelectQuery], visitor func(k, v []byte) error, vars ...any) error {
	rows, err := t.readPool.Query(context.Background(), query.Fn(), vars...)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = forEachRow(rows, visitor)
	return err
}

func rowsToRange(rows pgx.Rows) (keys [][]byte, vals [][]byte) {
	for rows.Next() {
		var key, value []byte
		err := rows.Scan(&key, &value)
		if err != nil {
			panic(err)
		}
		keys = append(keys, key)
		vals = append(vals, value)
	}
	return keys, vals
}

func (t *pgReadTx) unsafeRange(bucketType Bucket, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	t.assertNotKvAware(bucketType)
	if bucketType.IsKeys() && t.kvType == KvBucketKeys {
		return t.unsafeRangeShared(KEYS_RANGE, key, endKey, zeronull.Int8(limit))
	} else {
		return t.unsafeRangeShared(BUCKETS_RANGE, bucketType.Name(), key, endKey, zeronull.Int8(limit))
	}
}

func (t *pgReadTx) unsafeRangeShared(query pgFn[*dialect.SelectQuery], vars ...any) (keys [][]byte, vals [][]byte) {
	rows, err := t.readPool.Query(context.Background(), query.Fn(), vars...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	return rowsToRange(rows)
}

func (t *pgReadTx) unsafeKvRangeEntries(key, endKey []byte, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
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

func (t *pgReadTx) unsafeKvRangeEntriesShared(query pgFn[*dialect.SelectQuery], key, endKey []byte, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
	minCreateRev, maxCreateRev, minModRev, maxModRev := ro.toNullable()
	nLimit := zeronull.Int8(limit)
	rows, err := t.readPool.Query(context.Background(), query.Fn(), key, endKey, nLimit, minCreateRev, maxCreateRev, minModRev, maxModRev)
	if err != nil {
		t.lg.Panic("Error reading entries", zap.Error(err))
	}
	defer rows.Close()
	bIter := t.kvBuffer.RangeEntries(key, endKey)
	rIter := newRowRangeIter(t.lg, rows)
	return kvBufferJoinKeyValue(bIter, rIter, limit)
}

func (t *pgReadTx) unsafeKvLogRangeEntries(key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
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

func (t *pgReadTx) unsafeKvLogRangeEntriesShared(query pgFn[*dialect.SelectQuery], key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
	minCreateRev, maxCreateRev, minModRev, maxModRev := ro.toNullable()
	nLatestRev, nLimit := zeronull.Int8(latestRev), zeronull.Int8(limit)
	rows, err := t.readPool.Query(context.Background(), query.Fn(), key, endKey, nLatestRev, nLimit, minCreateRev, maxCreateRev, minModRev, maxModRev)
	if err != nil {
		t.lg.Panic("Error reading entries", zap.Error(err))
	}
	defer rows.Close()

	bIter := t.kvBuffer.RangeEntries(key, endKey)
	rIter := newRowRangeIter(t.lg, rows)
	return kvBufferJoinKeyValue(bIter, rIter, limit)
}

func kvBufferJoinKeyValue(bIter KVIter[string, *PgKvLogEntry], rIter RowRangeIter, limit int64) []mvccpb.KeyValue {
	var kvs []mvccpb.KeyValue
	bNext := bIter.Next()
	rNext := rIter.Next()

	for {
		if limit != 0 && int64(len(kvs)) > limit {
			break
		}
		if bNext && rNext {
			if bIter.Key() == rIter.Key() {
				entry := bIter.Value()
				if entry == nil {
					panic("empty entry")
				}
				kvs = append(kvs, entry.toKeyValue())
				bNext = bIter.Next()
				rNext = rIter.Next()
			} else if bIter.Key() < rIter.Key() {
				entry := bIter.Value()
				kvs = append(kvs, entry.toKeyValue())
				bNext = bIter.Next()
			} else {
				entry := rIter.Value()
				kvs = append(kvs, entry)
				rNext = rIter.Next()
			}
		} else if bNext {
			entry := bIter.Value()
			kvs = append(kvs, entry.toKeyValue())
			bNext = bIter.Next()
		} else if rNext {
			entry := rIter.Value()
			kvs = append(kvs, entry)
			rNext = rIter.Next()
		} else {
			break
		}
	}
	return kvs
}

func rowsToEntries(lg *zap.Logger, rows pgx.Rows) []mvccpb.KeyValue {
	kvs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (mvccpb.KeyValue, error) {
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
		return kv, err
	})
	if err != nil {
		lg.Panic("error scanning", zap.Error(err))
	}
	return kvs
}

func (t *pgReadTx) unsafeKvRangeKeys(key, endKey []byte, limit int64, ro KvRangeOptions) [][]byte {
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

func (t *pgReadTx) unsafeKvRangeKeysShared(query pgFn[*dialect.SelectQuery], key, endKey []byte, limit int64, ro KvRangeOptions) [][]byte {
	minCreateRev, maxCreateRev, minModRev, maxModRev := ro.toNullable()
	nLimit := zeronull.Int8(limit)
	rows, err := t.readPool.Query(context.Background(), query.Fn(), key, endKey, nLimit, minCreateRev, maxCreateRev, minModRev, maxModRev)
	if err != nil {
		t.lg.Panic("Error reading entries", zap.Error(err))
	}
	defer rows.Close()

	bIter := t.kvBuffer.RangeEntries(key, endKey)
	rIter := newRowRangeKeyIter(t.lg, rows)
	return kvBufferJoinKeys(bIter, rIter, limit)
}

func (t *pgReadTx) unsafeKvLogRangeKeys(key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) [][]byte {
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

func (t *pgReadTx) unsafeKvLogRangeKeysShared(query pgFn[*dialect.SelectQuery], key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) [][]byte {
	minCreateRev, maxCreateRev, minModRev, maxModRev := ro.toNullable()
	nLatestRev, nLimit := zeronull.Int8(latestRev), zeronull.Int8(limit)
	rows, err := t.readPool.Query(context.Background(), query.Fn(), key, endKey, nLatestRev, nLimit, minCreateRev, maxCreateRev, minModRev, maxModRev)
	if err != nil {
		t.lg.Panic("Error reading keys", zap.Error(err))
	}
	defer rows.Close()

	bIter := t.kvBuffer.RangeEntries(key, endKey)
	rIter := newRowRangeKeyIter(t.lg, rows)
	return kvBufferJoinKeys(bIter, rIter, limit)
}

func kvBufferJoinKeys(bIter KVIter[string, *PgKvLogEntry], rIter RowRangeKeyIter, limit int64) [][]byte {
	var keys [][]byte
	bNext := bIter.Next()
	rNext := rIter.Next()

	for {
		if limit != 0 && int64(len(keys)) > limit {
			break
		}
		if bNext && rNext {
			if bIter.Key() == rIter.Key() {
				entry := bIter.Value()
				keys = append(keys, entry.Key)
				bNext = bIter.Next()
				rNext = rIter.Next()
			} else if bIter.Key() < rIter.Key() {
				entry := bIter.Value()
				keys = append(keys, entry.Key)
				bNext = bIter.Next()
			} else {
				keys = append(keys, rIter.key)
				rNext = rIter.Next()
			}
		} else if bNext {
			entry := bIter.Value()
			keys = append(keys, entry.Key)
			bNext = bIter.Next()
		} else if rNext {
			keys = append(keys, rIter.key)
			rNext = rIter.Next()
		} else {
			break
		}
	}
	return keys
}

func rowsToKeys(lg *zap.Logger, rows pgx.Rows) [][]byte {
	keys, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		lg.Panic("error scanning", zap.Error(err))
	}
	return keys
}

func (t *pgReadTx) unsafeKvLogForEachByRev(latestRev int64, visitor func(entry mvccpb.KeyValue) error) error {
	switch t.kvType {
	case KvLbrNowNorm, KvLbrNowNifs:
		return t.unsafeKvLogForEachByRevShared(KV_LBR_ENTRY, latestRev, visitor)
	case KvLbrKidNowNorm, KvLbrKidNowNifs:
		return t.unsafeKvLogForEachByRevShared(KV_LBR_KID_ENTRY, latestRev, visitor)
	default:
		t.lg.Panic("Unexpected kvType", zap.String("kvType", t.kvType.String()))
	}
	// we panic, but go doesn't realize that
	return nil
}

func (t *pgReadTx) unsafeKvLogForEachByRevShared(query pgFn[*dialect.SelectQuery], latestRev int64, visitor func(entry mvccpb.KeyValue) error) error {
	nLatestRev := zeronull.Int8(latestRev)
	rows, err := t.readPool.Query(context.Background(), query.Fn(), nLatestRev)
	if err != nil {
		t.lg.Panic("Error reading keys", zap.Error(err))
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
