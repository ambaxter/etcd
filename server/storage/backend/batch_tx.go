// Copyright 2015 The etcd Authors
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
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"go.uber.org/zap"

	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

type BucketID int

type Bucket interface {
	// ID returns a unique identifier of a bucket.
	// The id must NOT be persisted and can be used as lightweight identificator
	// in the in-memory maps.
	ID() BucketID
	Name() []byte
	// String implements Stringer (human readable name).
	String() string

	// IsSafeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
	// overwrites on a bucket should only fetch with limit=1, but safeRangeBucket
	// is known to never overwrite any key so range is safe.
	IsSafeRangeBucket() bool

	IsKeys() bool
}

type BatchTx interface {
	Lock()
	Unlock()
	// Commit commits a previous tx and begins a new writable one.
	Commit()
	// CommitAndStop commits the previous tx and does not create a new one.
	CommitAndStop()
	LockInsideApply()
	LockOutsideApply()
	UnsafeReadWriter
}

type UnsafeReadWriter interface {
	UnsafeKvReader
	UnsafeKvWriter
}

type UnsafeWriter interface {
	UnsafeCreateBucket(bucket Bucket)
	UnsafeDeleteBucket(bucket Bucket)
	UnsafePut(bucket Bucket, key []byte, value []byte)
	UnsafeSeqPut(bucket Bucket, key []byte, value []byte)
	UnsafeDelete(bucket Bucket, key []byte)
}

type UnsafeKvWriter interface {
	UnsafeWriter
	UnsafeKvPutKey(revMain, revSub, revCreate, lease, version int64, key, value []byte)
	UnsafeKvDeleteKey(revMain, revSub int64, key []byte)
	LockKvCompact()
	UnlockKvCompact()
	UnsafeKvLogCompact(compactMainRev int64, visitor func(entry mvccpb.KeyValue) error) (int64, error)
}

type UnsafeKvReadWriter interface {
	UnsafeKvReader
	UnsafeKvWriter
}

type batchTx struct {
	sync.Mutex
	tx      *bolt.Tx
	backend *backend
	pgTx    *pgBatchTx
	pending int
}

func (t *batchTx) IsPgAware() bool {
	return t.pgTx != nil
}

func (t *batchTx) assertPgAware() {
	if !t.IsPgAware() {
		t.backend.lg.Panic("Attempting to use pg aware functions!")
	}
}

func (t *batchTx) IsKvAware() bool {
	return t.pgTx != nil && t.pgTx.IsKvAware()
}

func (t *batchTx) assertKvAware() {
	if !t.IsKvAware() {
		t.backend.lg.Panic("Attempting to use kv aware functions!")
	}
}

// Lock is supposed to be called only by the unit test.
func (t *batchTx) Lock() {
	ValidateCalledInsideUnittest(t.backend.lg)
	t.lock()
}

func (t *batchTx) lock() {
	t.Mutex.Lock()
}

func (t *batchTx) LockInsideApply() {
	t.lock()
	if t.backend.txPostLockInsideApplyHook != nil {
		// The callers of some methods (i.e., (*RaftCluster).AddMember)
		// can be coming from both InsideApply and OutsideApply, but the
		// callers from OutsideApply will have a nil txPostLockInsideApplyHook.
		// So we should check the txPostLockInsideApplyHook before validating
		// the callstack.
		ValidateCalledInsideApply(t.backend.lg)
		t.backend.txPostLockInsideApplyHook()
	}
}

func (t *batchTx) LockOutsideApply() {
	ValidateCalledOutSideApply(t.backend.lg)
	t.lock()
}

func (t *batchTx) Unlock() {
	if t.pending >= t.backend.batchLimit {
		t.commit(false)
	}
	t.Mutex.Unlock()
}

func (t *batchTx) LockKvCompact() {
	if t.IsKvAware() {
		t.pgTx.compactLock.Lock()
	}
}

func (t *batchTx) UnlockKvCompact() {
	if t.IsKvAware() {
		t.pgTx.compactLock.Unlock()
	}
}

func (t *batchTx) UnsafeCreateBucket(bucket Bucket) {
	if t.pgTx == nil {
		if _, err := t.tx.CreateBucketIfNotExists(bucket.Name()); err != nil {
			t.backend.lg.Fatal(
				"failed to create a bucket",
				zap.Stringer("bucket-name", bucket),
				zap.Error(err),
			)
		}
	} else {
		t.pgTx.unsafeCreateBucket(bucket)
	}
	t.pending++
}

func (t *batchTx) UnsafeDeleteBucket(bucket Bucket) {
	if t.pgTx == nil {
		err := t.tx.DeleteBucket(bucket.Name())
		if err != nil && !errors.Is(err, bolterrors.ErrBucketNotFound) {
			t.backend.lg.Fatal(
				"failed to delete a bucket",
				zap.Stringer("bucket-name", bucket),
				zap.Error(err),
			)
		}
	} else {
		t.pgTx.unsafeDeleteBucket(bucket)
	}

	t.pending++
}

// UnsafePut must be called holding the lock on the tx.
func (t *batchTx) UnsafePut(bucket Bucket, key []byte, value []byte) {
	t.unsafePut(bucket, key, value, false)
}

// UnsafeSeqPut must be called holding the lock on the tx.
func (t *batchTx) UnsafeSeqPut(bucket Bucket, key []byte, value []byte) {
	t.unsafePut(bucket, key, value, true)
}

func (t *batchTx) unsafePut(bucketType Bucket, key []byte, value []byte, seq bool) {
	if t.pgTx == nil {
		bucket := t.tx.Bucket(bucketType.Name())
		if bucket == nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.Stringer("bucket-name", bucketType),
				zap.Stack("stack"),
			)
		}
		if seq {
			// it is useful to increase fill percent when the workloads are mostly append-only.
			// this can delay the page split and reduce space usage.
			bucket.FillPercent = 0.9
		}
		if err := bucket.Put(key, value); err != nil {
			t.backend.lg.Fatal(
				"failed to write to a bucket",
				zap.Stringer("bucket-name", bucketType),
				zap.Error(err),
			)
		}
	} else {
		t.pgTx.unsafePutShared(bucketType, key, value)
	}
	t.pending++
}

// UnsafeRange must be called holding the lock on the tx.
func (t *batchTx) UnsafeRange(bucketType Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if t.pgTx == nil {
		bucket := t.tx.Bucket(bucketType.Name())
		if bucket == nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.Stringer("bucket-name", bucketType),
				zap.Stack("stack"),
			)
		}
		return unsafeRange(bucket.Cursor(), key, endKey, limit)
	} else {
		return t.pgTx.unsafeRange(bucketType, key, endKey, limit)
	}

}

func unsafeRange(c *bolt.Cursor, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	if limit <= 0 {
		limit = math.MaxInt64
	}
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() {
		vs = append(vs, cv)
		keys = append(keys, ck)
		if limit == int64(len(keys)) {
			break
		}
	}
	return keys, vs
}

// UnsafeDelete must be called holding the lock on the tx.
func (t *batchTx) UnsafeDelete(bucketType Bucket, key []byte) {
	if t.pgTx == nil {
		bucket := t.tx.Bucket(bucketType.Name())
		if bucket == nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.Stringer("bucket-name", bucketType),
				zap.Stack("stack"),
			)
		}
		err := bucket.Delete(key)
		if err != nil {
			t.backend.lg.Fatal(
				"failed to delete a key",
				zap.Stringer("bucket-name", bucketType),
				zap.Error(err),
			)
		}
	} else {
		t.pgTx.unsafeDelete(bucketType, key)
	}
	t.pending++
}

// UnsafeForEach must be called holding the lock on the tx.
func (t *batchTx) UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	if t.pgTx == nil {
		return unsafeForEach(t.tx, bucket, visitor)
	} else {
		return t.pgTx.unsafeForEach(bucket, visitor)
	}

}

func unsafeForEach(tx *bolt.Tx, bucket Bucket, visitor func(k, v []byte) error) error {
	if b := tx.Bucket(bucket.Name()); b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

func (t *batchTx) UnsafeExactKeys(bucket Bucket, keys [][]byte) (vals map[string][]byte) {
	t.assertPgAware()
	vals = make(map[string][]byte, len(keys))
	return t.pgTx.unsafeExactKeys(bucket, keys, vals)
}

func (t *batchTx) UnsafeKvRangeEntries(key, endKey []byte, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
	return t.pgTx.unsafeKvRangeEntries(key, endKey, limit, ro)
}

func (t *batchTx) UnsafeKvLogRangeEntries(key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) []mvccpb.KeyValue {
	return t.pgTx.unsafeKvLogRangeEntries(key, endKey, latestRev, limit, ro)
}

func (t *batchTx) UnsafeKvRangeKeys(key, endKey []byte, limit int64, ro KvRangeOptions) [][]byte {
	return t.pgTx.unsafeKvRangeKeys(key, endKey, limit, ro)
}

func (t *batchTx) UnsafeKvLogRangeKeys(key, endKey []byte, latestRev int64, limit int64, ro KvRangeOptions) [][]byte {
	return t.pgTx.unsafeKvLogRangeKeys(key, endKey, latestRev, limit, ro)
}

func (t *batchTx) UnsafeKvLogForEachByRev(latestRev int64, visitor func(entry mvccpb.KeyValue) error) error {
	t.assertKvAware()
	return t.pgTx.unsafeKvLogForEachByRev(latestRev, visitor)
}

func (t *batchTx) UnsafeKvPutKey(revMain, revSub, revCreate, lease, version int64, key, value []byte) {
	entry := &PgKvLogEntry{
		RevMain:   revMain,
		RevSub:    revSub,
		RevCreate: revCreate,
		Lease:     zeronull.Int8(lease),
		Version:   version,
		Key:       key,
		Value:     value,
	}
	t.unsafeKvPutKeyShared(entry)
}

func (t *batchTx) unsafeKvPutKeyShared(entry *PgKvLogEntry) {
	t.pgTx.unsafeKvPutKey(entry)
	t.pending += 1
}

func (t *batchTx) UnsafeKvDeleteKey(revMain, revSub int64, key []byte) {
	t.pgTx.unsafeKvDeleteKey(revMain, revSub, key)
	t.pending += 1
}

func (t *batchTx) UnsafeKvLogCompact(compactMainRev int64, visitor func(entry mvccpb.KeyValue) error) (int64, error) {
	return t.pgTx.unsafeKvLogCompact(compactMainRev, visitor)
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTx) Commit() {
	t.lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTx) CommitAndStop() {
	t.lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTx) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.pending
}

func (t *batchTx) commit(stop bool) {
	// commit the last tx
	if t.tx != nil || (t.pgTx != nil && t.pgTx.tx != nil) {
		if t.pending == 0 && !stop {
			return
		}

		start := time.Now()
		var err error
		// gofail: var beforeCommit struct{}
		if t.pgTx == nil {
			err = t.tx.Commit()
			rebalanceSec.Observe(t.tx.Stats().RebalanceTime.Seconds())
			spillSec.Observe(t.tx.Stats().SpillTime.Seconds())
			writeSec.Observe(t.tx.Stats().WriteTime.Seconds())
		} else {
			t.pgTx.updateDbTx()
			err = t.pgTx.tx.Commit(context.Background())
			t.pgTx.tx = nil
			t.pgTx.txBatch = nil
		}
		// gofail: var afterCommit struct{}

		commitSec.Observe(time.Since(start).Seconds())
		atomic.AddInt64(&t.backend.commits, 1)

		t.pending = 0
		if err != nil {
			t.backend.lg.Fatal("failed to commit tx", zap.Error(err))
		}
	}
	if !stop {
		if t.pgTx == nil {
			t.tx = t.backend.begin(true)
		} else {
			t.pgTx.tx = t.backend.pgBeginWrite()
			t.pgTx.txBatch = &pgx.Batch{}
		}

	}
}

type batchTxBuffered struct {
	batchTx
	buf                     txWriteBuffer
	kvBuf                   PgKvBuffer[*PgKvLogEntry]
	pendingDeleteOperations int
}

func newBatchTxBuffered(backend *backend) *batchTxBuffered {
	tx := &batchTxBuffered{
		batchTx: batchTx{backend: backend},
		buf: txWriteBuffer{
			txBuffer:   txBuffer{make(map[BucketID]*bucketBuffer)},
			bucket2seq: make(map[BucketID]bool),
		},
	}
	tx.Commit()
	return tx
}

func newPgBatchTxBuffered(backend *backend) *batchTxBuffered {
	lg := backend.lg
	tx := &batchTxBuffered{
		batchTx: batchTx{
			backend: backend,
			pgTx: &pgBatchTx{
				pgSharedTx: pgSharedTx{
					lg:     lg,
					kvType: backend.pgBackend.kvType,
				},
				backend:    backend.pgBackend,
				subDbBatch: newPgDbBatch(lg, backend.pgBackend.kvType),
				subKvBatch: newPgKvBatch(lg),
				txBatch:    &pgx.Batch{},
			},
		},
		buf: txWriteBuffer{
			txBuffer:   txBuffer{make(map[BucketID]*bucketBuffer)},
			bucket2seq: make(map[BucketID]bool),
		},
	}
	tx.Commit()
	return tx
}

func (t *batchTxBuffered) Unlock() {
	if t.pending != 0 {
		t.backend.readTx.Lock() // blocks txReadBuffer for writing.
		// gofail: var beforeWritebackBuf struct{}
		t.buf.writeback(&t.backend.readTx.buf)
		if t.pgTx != nil {
			iMain := t.kvBuf.m.Iter()
			for iMain.Next() {
				subTree := iMain.Value()
				iSub := subTree.Iter()
				for iSub.Next() {
					entry := iSub.Value()
					t.backend.readTx.pgTx.kvBuffer.Put(entry.Key, entry.RevMain, entry)
				}
			}
			t.kvBuf.Clear()
		}
		// gofail: var afterWritebackBuf struct{}
		t.backend.readTx.Unlock()
		// We commit the transaction when the number of pending operations
		// reaches the configured limit(batchLimit) to prevent it from
		// becoming excessively large.
		//
		// But we also need to commit the transaction immediately if there
		// is any pending deleting operation, otherwise etcd might run into
		// a situation that it haven't finished committing the data into backend
		// storage (note: etcd periodically commits the bbolt transactions
		// instead of on each request) when it applies next request. Accordingly,
		// etcd may still read the stale data from bbolt when processing next
		// request. So it breaks the linearizability.
		//
		// Note we don't need to commit the transaction for put requests if
		// it doesn't exceed the batch limit, because there is a buffer on top
		// of the bbolt. Each time when etcd reads data from backend storage,
		// it will read data from both bbolt and the buffer. But there is no
		// such a buffer for delete requests.
		//
		// Please also refer to
		// https://github.com/etcd-io/etcd/pull/17119#issuecomment-1857547158
		if t.pending >= t.backend.batchLimit || t.pendingDeleteOperations > 0 {
			t.commit(false)
		}
	}
	t.batchTx.Unlock()
}

func (t *batchTxBuffered) Commit() {
	t.lock()
	t.commit(false)
	t.Unlock()
}

func (t *batchTxBuffered) CommitAndStop() {
	t.lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTxBuffered) commit(stop bool) {
	// all read txs must be closed to acquire boltdb commit rwlock
	t.backend.readTx.Lock()
	t.unsafeCommit(stop)
	t.backend.readTx.Unlock()
}

func (t *batchTxBuffered) unsafeCommit(stop bool) {
	if t.backend.hooks != nil {
		// gofail: var commitBeforePreCommitHook struct{}
		t.backend.hooks.OnPreCommitUnsafe(t)
		// gofail: var commitAfterPreCommitHook struct{}
	}

	if t.backend.readTx.tx != nil {
		// wait all store read transactions using the current boltdb tx to finish,
		// then close the boltdb tx
		go func(tx *bolt.Tx, wg *sync.WaitGroup) {
			wg.Wait()
			if err := tx.Rollback(); err != nil {
				t.backend.lg.Fatal("failed to rollback tx", zap.Error(err))
			}
		}(t.backend.readTx.tx, t.backend.readTx.txWg)
		t.backend.readTx.reset()
	} else if t.backend.readTx.pgTx != nil {
		go func(wg *sync.WaitGroup) {
			wg.Wait()
		}(t.backend.readTx.txWg)
		t.backend.readTx.reset()
	}

	t.batchTx.commit(stop)
	t.pendingDeleteOperations = 0

	if !stop {
		if t.pgTx == nil {
			t.backend.readTx.tx = t.backend.begin(false)
		}
	}
}

func (t *batchTxBuffered) UnsafePut(bucket Bucket, key []byte, value []byte) {
	t.batchTx.UnsafePut(bucket, key, value)
	t.buf.put(bucket, key, value)
}

func (t *batchTxBuffered) UnsafeSeqPut(bucket Bucket, key []byte, value []byte) {
	t.batchTx.UnsafeSeqPut(bucket, key, value)
	t.buf.putSeq(bucket, key, value)
}

func (t *batchTxBuffered) UnsafeDelete(bucketType Bucket, key []byte) {
	t.batchTx.UnsafeDelete(bucketType, key)
	t.pendingDeleteOperations++
}

func (t *batchTxBuffered) UnsafeDeleteBucket(bucket Bucket) {
	t.batchTx.UnsafeDeleteBucket(bucket)
	t.pendingDeleteOperations++
}

func (t *batchTxBuffered) UnsafeKvPutKey(revMain, revSub, revCreate, lease, version int64, key, value []byte) {
	entry := &PgKvLogEntry{
		RevMain:   revMain,
		RevSub:    revSub,
		RevCreate: revCreate,
		Lease:     zeronull.Int8(lease),
		Version:   version,
		Key:       key,
		Value:     value,
	}
	t.batchTx.unsafeKvPutKeyShared(entry)
	t.kvBuf.Put(entry.Key, entry.RevMain, entry)
}

func (t *batchTxBuffered) UnsafeKvDeleteKey(revMain, revSub int64, key []byte) {
	t.batchTx.UnsafeKvDeleteKey(revMain, revSub, key)
	t.pendingDeleteOperations++
}

func (t *batchTxBuffered) UnsafeKvLogCompact(compactMainRev int64, visitor func(entry mvccpb.KeyValue) error) (int64, error) {
	c, err := t.batchTx.UnsafeKvLogCompact(compactMainRev, visitor)
	t.pendingDeleteOperations++
	return c, err
}
