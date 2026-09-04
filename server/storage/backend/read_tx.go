// Copyright 2017 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"math"
	"sync"
	"unsafe"

	"github.com/jackc/pgx/v5/pgtype/zeronull"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
)

// IsSafeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
// overwrites on a bucket should only fetch with limit=1, but IsSafeRangeBucket
// is known to never overwrite any key so range is safe.

type ReadTx interface {
	RLock()
	RUnlock()
	UnsafeKvReader
}

type UnsafeReader interface {
	UnsafeRange(bucket Bucket, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)
	UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error
}

type UnsafePgReader interface {
	UnsafeReader
	IsPgAware() bool
	UnsafeExactKeys(bucket Bucket, keys [][]byte) (vals map[string][]byte)
}

type UnsafeKvReader interface {
	UnsafePgReader
	IsKvAware() bool
	UnsafeKvRangeEntries(key, endKey []byte, limit int64, ro KvRangeOptions) []mvccpb.KeyValue
	UnsafeKvLogRangeEntries(key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) []mvccpb.KeyValue
	UnsafeKvRangeKeys(key, endKey []byte, limit int64, ro KvRangeOptions) [][]byte
	UnsafeKvLogRangeKeys(key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) [][]byte
	UnsafeKvLogForEachByRev(latestRev int64, visitor func(entry mvccpb.KeyValue) error) error
}

type KvRangeOptions struct {
	MinCreateRev int64
	MaxCreateRev int64
	MinModRev    int64
	MaxModRev    int64
}

func (o KvRangeOptions) toNullable() (minCreateRev, maxCreateRev, minModRev, maxModRev zeronull.Int8) {
	minCreateRev = zeronull.Int8(o.MinCreateRev)
	maxCreateRev = zeronull.Int8(o.MaxCreateRev)
	minModRev = zeronull.Int8(o.MinModRev)
	maxModRev = zeronull.Int8(o.MaxModRev)
	return minCreateRev, maxCreateRev, minModRev, maxModRev
}

type KvVersion struct {
	CreateRevision int64
	Version        int64
}

func (kv *KvVersion) isTombstone() bool {
	return kv.CreateRevision == 0 && kv.Version == 0
}

// Base type for readTx and concurrentReadTx to eliminate duplicate functions between these
type baseReadTx struct {
	// because we should always have a logger
	lg *zap.Logger

	// mu protects accesses to the txReadBuffer
	mu  sync.RWMutex
	buf txReadBuffer

	// TODO: group and encapsulate {txMu, tx, buckets, txWg}, as they share the same lifecycle.
	// txMu protects accesses to buckets and tx on Range requests.
	txMu    *sync.RWMutex
	tx      *bolt.Tx
	buckets map[BucketID]*bolt.Bucket
	// txWg protects tx from being rolled back at the end of a batch interval until all reads using this tx are done.
	txWg *sync.WaitGroup

	pgTx *pgReadTx
}

func (baseReadTx *baseReadTx) IsPgAware() bool {
	return baseReadTx.pgTx != nil
}

func (baseReadTx *baseReadTx) assertPgAware() {
	if !baseReadTx.IsPgAware() {
		baseReadTx.lg.Panic("Attempting to use pg aware functions!")
	}
}

func (baseReadTx *baseReadTx) IsKvAware() bool {
	return baseReadTx.pgTx != nil && baseReadTx.pgTx.IsKvAware()
}

func (baseReadTx *baseReadTx) assertKvAware() {
	if !baseReadTx.IsKvAware() {
		baseReadTx.lg.Panic("Attempting to use kv aware functions!")
	}
}

func (baseReadTx *baseReadTx) UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
	if err := baseReadTx.buf.ForEach(bucket, getDups); err != nil {
		return err
	}
	baseReadTx.txMu.Lock()
	var err error
	if baseReadTx.pgTx == nil {
		err = unsafeForEach(baseReadTx.tx, bucket, visitNoDup)
	} else {
		err = baseReadTx.pgTx.unsafeForEach(bucket, visitNoDup)
	}
	baseReadTx.txMu.Unlock()
	if err != nil {
		return err
	}
	return baseReadTx.buf.ForEach(bucket, visitor)
}

func (baseReadTx *baseReadTx) UnsafeRange(bucketType Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	if limit > 1 && !bucketType.IsSafeRangeBucket() {
		panic("do not use unsafeRange on non-keys bucket")
	}
	keys, vals := baseReadTx.buf.Range(bucketType, key, endKey, limit)
	if int64(len(keys)) == limit {
		return keys, vals
	}

	if baseReadTx.pgTx == nil {
		// find/cache bucket
		bn := bucketType.ID()
		baseReadTx.txMu.RLock()
		bucket, ok := baseReadTx.buckets[bn]
		baseReadTx.txMu.RUnlock()
		lockHeld := false
		if !ok {
			baseReadTx.txMu.Lock()
			lockHeld = true
			bucket = baseReadTx.tx.Bucket(bucketType.Name())
			baseReadTx.buckets[bn] = bucket
		}

		// ignore missing bucket since may have been created in this batch
		if bucket == nil {
			if lockHeld {
				baseReadTx.txMu.Unlock()
			}
			return keys, vals
		}
		if !lockHeld {
			baseReadTx.txMu.Lock()
		}
		c := bucket.Cursor()
		baseReadTx.txMu.Unlock()

		k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))
		return append(k2, keys...), append(v2, vals...)
	} else {
		k2, v2 := baseReadTx.pgTx.unsafeRange(bucketType, key, endKey, limit-int64(len(keys)))
		return append(k2, keys...), append(v2, vals...)
	}
}

func (t *baseReadTx) UnsafeExactKeys(bucket Bucket, keys [][]byte) (vals map[string][]byte) {
	t.assertPgAware()
	vals = make(map[string][]byte, len(keys))
	var inDb [][]byte
	for _, key := range keys {
		_, cVal := t.buf.Range(bucket, key, nil, 1)
		if len(cVal) == 1 {
			strKey := unsafe.String(unsafe.SliceData(key), len(key))
			vals[strKey] = cVal[0]
		} else {
			inDb = append(inDb, key)
		}
	}
	if len(inDb) == 0 {
		return vals
	}
	return t.pgTx.unsafeExactKeys(bucket, inDb, vals)
}

func (t *baseReadTx) UnsafeKvRangeEntries(key, endKey []byte, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
	t.assertKvAware()
	return t.pgTx.unsafeKvRangeEntries(key, endKey, limit, ro)
}

func (t *baseReadTx) UnsafeKvLogRangeEntries(key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
	t.assertKvAware()
	return t.pgTx.unsafeKvLogRangeEntries(key, endKey, latestRev, limit, ro)
}

func (t *baseReadTx) UnsafeKvRangeKeys(key, endKey []byte, limit int64, ro KvRangeOptions) [][]byte {
	t.assertKvAware()
	return t.pgTx.unsafeKvRangeKeys(key, endKey, limit, ro)
}

func (t *baseReadTx) UnsafeKvLogRangeKeys(key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) [][]byte {
	t.assertKvAware()
	return t.pgTx.unsafeKvLogRangeKeys(key, endKey, latestRev, limit, ro)
}

func (t *baseReadTx) UnsafeKvLogForEachByRev(latestRev int64, visitor func(entry mvccpb.KeyValue) error) error {
	t.assertKvAware()
	return t.pgTx.unsafeKvLogForEachByRev(latestRev, visitor)
}

type readTx struct {
	baseReadTx
}

func (rt *readTx) Lock()    { rt.mu.Lock() }
func (rt *readTx) Unlock()  { rt.mu.Unlock() }
func (rt *readTx) RLock()   { rt.mu.RLock() }
func (rt *readTx) RUnlock() { rt.mu.RUnlock() }

func (rt *readTx) reset() {
	rt.buf.reset()
	rt.buckets = make(map[BucketID]*bolt.Bucket)
	rt.tx = nil
	rt.txWg = new(sync.WaitGroup)
	if rt.pgTx != nil {
		rt.pgTx.kvBuffer.Clear()
	}
}

type concurrentReadTx struct {
	baseReadTx
}

func (rt *concurrentReadTx) Lock()   {}
func (rt *concurrentReadTx) Unlock() {}

// RLock is no-op. concurrentReadTx does not need to be locked after it is created.
func (rt *concurrentReadTx) RLock() {}

// RUnlock signals the end of concurrentReadTx.
func (rt *concurrentReadTx) RUnlock() { rt.txWg.Done() }
