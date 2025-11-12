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

package mvcc

import (
	"context"
	"fmt"
	"unsafe"

	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

type storeTxnRead struct {
	storeTxnCommon
	tx backend.ReadTx
}

type storeTxnCommon struct {
	s  *store
	tx backend.UnsafeKvReader

	firstRev int64
	rev      int64

	trace *traceutil.Trace
}

func (s *store) Read(mode ReadTxMode, trace *traceutil.Trace) TxnRead {
	s.mu.RLock()
	s.revMu.RLock()
	// For read-only workloads, we use shared buffer by copying transaction read buffer
	// for higher concurrency with ongoing blocking writes.
	// For write/write-read transactions, we use the shared buffer
	// rather than duplicating transaction read buffer to avoid transaction overhead.
	var tx backend.ReadTx
	if mode == ConcurrentReadTxMode {
		tx = s.b.ConcurrentReadTx()
	} else {
		tx = s.b.ReadTx()
	}

	tx.RLock() // RLock is no-op. concurrentReadTx does not need to be locked after it is created.
	firstRev, rev := s.compactMainRev, s.currentRev
	s.revMu.RUnlock()
	return newMetricsTxnRead(&storeTxnRead{storeTxnCommon{s, tx, firstRev, rev, trace}, tx})
}

func (tr *storeTxnCommon) FirstRev() int64 { return tr.firstRev }
func (tr *storeTxnCommon) Rev() int64      { return tr.rev }

func (tr *storeTxnCommon) Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	if tr.tx.IsKvAware() {
		return tr.kvRangeKeys(ctx, key, end, tr.Rev(), ro)
	} else {
		return tr.rangeKeys(ctx, key, end, tr.Rev(), ro)
	}

}

func (tr *storeTxnCommon) rangeKeys(ctx context.Context, key, end []byte, curRev int64, ro RangeOptions) (*RangeResult, error) {
	rev := ro.Rev
	if rev > curRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: curRev}, ErrFutureRev
	}
	if rev <= 0 {
		rev = curRev
	}
	if rev < tr.s.compactMainRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: 0}, ErrCompacted
	}
	if ro.Count {
		total := tr.s.kvindex.CountRevisions(key, end, rev)
		tr.trace.Step("count revisions from in-memory index tree")
		return &RangeResult{KVs: nil, Count: total, Rev: curRev}, nil
	}
	revpairs, total := tr.s.kvindex.Revisions(key, end, rev, int(ro.Limit))
	tr.trace.Step("range keys from in-memory index tree")
	if len(revpairs) == 0 {
		return &RangeResult{KVs: nil, Count: total, Rev: curRev}, nil
	}

	limit := int(ro.Limit)
	if limit <= 0 || limit > len(revpairs) {
		limit = len(revpairs)
	}

	kvs := make([]mvccpb.KeyValue, limit)

	if tr.tx.IsPgAware() {
		revKeys := make([][]byte, limit)
		for i, revpair := range revpairs[:len(kvs)] {
			revBytes := NewRevBytes()
			revKeys[i] = RevToBytes(revpair, revBytes)
		}
		vals := tr.tx.UnsafeExactKeys(schema.Key, revKeys)
		for i, k := range revKeys {
			strKey := unsafe.String(unsafe.SliceData(k), len(k))
			v := vals[strKey]
			if v == nil {
				revpair := revpairs[i]
				tr.s.lg.Fatal(
					"range failed to find revision pair",
					zap.Int64("revision-main", revpair.Main),
					zap.Int64("revision-sub", revpair.Sub),
					zap.Int64("revision-current", curRev),
					zap.Int64("range-option-rev", ro.Rev),
					zap.Int64("range-option-limit", ro.Limit),
					zap.Binary("key", key),
					zap.Binary("end", end),
					zap.Int("len-revpairs", len(revpairs)),
					zap.Int("len-values", len(vals)),
				)
			}
			if err := kvs[i].Unmarshal(v); err != nil {
				tr.s.lg.Fatal(
					"failed to unmarshal mvccpb.KeyValue",
					zap.Error(err),
				)
			}
		}
	} else {
		for i, revpair := range revpairs[:len(kvs)] {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("rangeKeys: context cancelled: %w", ctx.Err())
			default:
			}
			revBytes := NewRevBytes()
			revBytes = RevToBytes(revpair, revBytes)
			_, vs := tr.tx.UnsafeRange(schema.Key, revBytes, nil, 0)
			if len(vs) != 1 {
				tr.s.lg.Fatal(
					"range failed to find revision pair",
					zap.Int64("revision-main", revpair.Main),
					zap.Int64("revision-sub", revpair.Sub),
					zap.Int64("revision-current", curRev),
					zap.Int64("range-option-rev", ro.Rev),
					zap.Int64("range-option-limit", ro.Limit),
					zap.Binary("key", key),
					zap.Binary("end", end),
					zap.Int("len-revpairs", len(revpairs)),
					zap.Int("len-values", len(vs)),
				)
			}
			if err := kvs[i].Unmarshal(vs[0]); err != nil {
				tr.s.lg.Fatal(
					"failed to unmarshal mvccpb.KeyValue",
					zap.Error(err),
				)
			}
		}
	}
	if tr.tx.IsPgAware() {
		tr.trace.Step("range keys from pg db")
	} else {
		tr.trace.Step("range keys from bolt db")
	}
	return &RangeResult{KVs: kvs, Count: total, Rev: curRev}, nil
}

func (tr *storeTxnCommon) kvRangeKeys(_ context.Context, key, end []byte, curRev int64, ro RangeOptions) (*RangeResult, error) {
	rev := ro.Rev
	if rev > curRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: curRev}, ErrFutureRev
	}
	if rev <= 0 {
		rev = curRev
	}
	if rev < tr.s.compactMainRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: 0}, ErrCompacted
	}
	if ro.Count {
		total := tr.s.pgKvIndex.CountRevisions(key, end, rev)
		tr.trace.Step("count revisions from in-memory index tree")
		return &RangeResult{KVs: nil, Count: int(total), Rev: curRev}, nil
	}
	tr.trace.Step("range keys from in-memory index tree")

	var kvs []mvccpb.KeyValue

	if rev == curRev {
		kvs = tr.tx.UnsafeKvRangeEntries(key, end, ro.Limit, backend.KvRangeOptions{})
	} else {
		kvs = tr.tx.UnsafeKvLogRangeEntries(key, end, rev, ro.Limit, backend.KvRangeOptions{})
	}

	return &RangeResult{KVs: kvs, Count: len(kvs), Rev: curRev}, nil
}

func (tr *storeTxnRead) End() {
	tr.tx.RUnlock() // RUnlock signals the end of concurrentReadTx.
	tr.s.mu.RUnlock()
}

type storeTxnWrite struct {
	storeTxnCommon
	tx backend.BatchTx
	// beginRev is the revision where the txn begins; it will write to the next revision.
	beginRev int64
	changes  []mvccpb.KeyValue
}

func (s *store) Write(trace *traceutil.Trace) TxnWrite {
	s.mu.RLock()
	tx := s.b.BatchTx()
	tx.LockInsideApply()
	tw := &storeTxnWrite{
		storeTxnCommon: storeTxnCommon{s, tx, 0, 0, trace},
		tx:             tx,
		beginRev:       s.currentRev,
		changes:        make([]mvccpb.KeyValue, 0, 4),
	}
	return newMetricsTxnWrite(tw)
}

func (tw *storeTxnWrite) Rev() int64 { return tw.beginRev }

func (tw *storeTxnWrite) Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	rev := tw.beginRev
	if len(tw.changes) > 0 {
		rev++
	}
	if tw.tx.IsKvAware() {
		return tw.kvRangeKeys(ctx, key, end, rev, ro)
	} else {
		return tw.rangeKeys(ctx, key, end, rev, ro)
	}
}

func (tw *storeTxnWrite) DeleteRange(key, end []byte) (int64, int64) {
	var n int64
	if tw.tx.IsKvAware() {
		n = tw.kvDeleteRange(key, end)
	} else {
		n = tw.deleteRange(key, end)
	}
	if n != 0 || len(tw.changes) > 0 {
		return n, tw.beginRev + 1
	}
	return 0, tw.beginRev
}

func (tw *storeTxnWrite) Put(key, value []byte, lease lease.LeaseID) int64 {
	if tw.tx.IsKvAware() {
		tw.kvPut(key, value, lease)
	} else {
		tw.put(key, value, lease)
	}

	return tw.beginRev + 1
}

func (tw *storeTxnWrite) End() {
	// only update index if the txn modifies the mvcc state.
	if len(tw.changes) != 0 {
		// hold revMu lock to prevent new read txns from opening until writeback.
		tw.s.revMu.Lock()
		tw.s.currentRev++
	}
	tw.tx.Unlock()
	if len(tw.changes) != 0 {
		tw.s.revMu.Unlock()
	}
	tw.s.mu.RUnlock()
}

func (tw *storeTxnWrite) put(key, value []byte, leaseID lease.LeaseID) {
	rev := tw.beginRev + 1
	c := rev
	oldLease := lease.NoLease

	// if the key exists before, use its previous created and
	// get its previous leaseID
	_, created, ver, err := tw.s.kvindex.Get(key, rev)
	if err == nil {
		c = created.Main
		oldLease = tw.s.le.GetLease(lease.LeaseItem{Key: string(key)})
		tw.trace.Step("get key's previous created_revision and leaseID")
	}
	ibytes := NewRevBytes()
	idxRev := Revision{Main: rev, Sub: int64(len(tw.changes))}
	ibytes = RevToBytes(idxRev, ibytes)

	ver = ver + 1
	kv := mvccpb.KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: c,
		ModRevision:    rev,
		Version:        ver,
		Lease:          int64(leaseID),
	}

	d, err := kv.Marshal()
	if err != nil {
		tw.storeTxnCommon.s.lg.Fatal(
			"failed to marshal mvccpb.KeyValue",
			zap.Error(err),
		)
	}

	tw.trace.Step("marshal mvccpb.KeyValue")
	tw.tx.UnsafeSeqPut(schema.Key, ibytes, d)
	tw.s.kvindex.Put(key, idxRev)
	tw.changes = append(tw.changes, kv)
	tw.trace.Step("store kv pair into bolt db")

	if oldLease == leaseID {
		tw.trace.Step("attach lease to kv pair")
		return
	}

	if oldLease != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to detach lease")
		}
		err = tw.s.le.Detach(oldLease, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			tw.storeTxnCommon.s.lg.Error(
				"failed to detach old lease from a key",
				zap.Error(err),
			)
		}
	}
	if leaseID != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to attach lease")
		}
		err = tw.s.le.Attach(leaseID, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			panic("unexpected error from lease Attach")
		}
	}
	tw.trace.Step("attach lease to kv pair")
}

func (tw *storeTxnWrite) kvPut(key, value []byte, leaseID lease.LeaseID) {
	rev := tw.beginRev + 1
	createRev, version, ok := tw.s.pgKvIndex.Get(key, rev)
	if !ok {
		version = 1
		createRev = rev
	}

	oldLease := tw.s.le.GetLease(lease.LeaseItem{Key: string(key)})
	tw.trace.Step("get key's previous created_revision and leaseID")

	kv := mvccpb.KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: createRev,
		ModRevision:    rev,
		Version:        version,
		Lease:          int64(leaseID),
	}

	tw.trace.Step("marshal mvccpb.KeyValue")
	tw.tx.UnsafeKvPutKey(rev, int64(len(tw.changes)), createRev, int64(leaseID), version, key, value)
	tw.s.pgKvIndex.Put(key, rev, createRev, version)
	tw.changes = append(tw.changes, kv)
	tw.trace.Step("store kv pair into bolt db")

	if oldLease == leaseID {
		tw.trace.Step("attach lease to kv pair")
		return
	}

	if oldLease != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to detach lease")
		}
		err := tw.s.le.Detach(oldLease, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			tw.storeTxnCommon.s.lg.Error(
				"failed to detach old lease from a key",
				zap.Error(err),
			)
		}
	}
	if leaseID != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to attach lease")
		}
		err := tw.s.le.Attach(leaseID, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			panic("unexpected error from lease Attach")
		}
	}
	tw.trace.Step("attach lease to kv pair")
}

func (tw *storeTxnWrite) deleteRange(key, end []byte) int64 {
	rrev := tw.beginRev
	if len(tw.changes) > 0 {
		rrev++
	}
	keys, _ := tw.s.kvindex.Range(key, end, rrev)
	if len(keys) == 0 {
		return 0
	}
	for _, key := range keys {
		tw.delete(key)
	}
	return int64(len(keys))
}

func (tw *storeTxnWrite) kvDeleteRange(key, end []byte) int64 {
	rrev := tw.beginRev
	if len(tw.changes) > 0 {
		rrev++
	}
	keys := tw.s.pgKvIndex.KeyRange(key, end, rrev)
	if len(keys) == 0 {
		return 0
	}
	for _, key := range keys {
		tw.kvDelete(key)
	}
	return int64(len(keys))
}

func (tw *storeTxnWrite) delete(key []byte) {
	ibytes := NewRevBytes()
	idxRev := newBucketKey(tw.beginRev+1, int64(len(tw.changes)), true)
	ibytes = BucketKeyToBytes(idxRev, ibytes)

	kv := mvccpb.KeyValue{Key: key}

	d, err := kv.Marshal()
	if err != nil {
		tw.storeTxnCommon.s.lg.Fatal(
			"failed to marshal mvccpb.KeyValue",
			zap.Error(err),
		)
	}

	tw.tx.UnsafeSeqPut(schema.Key, ibytes, d)
	err = tw.s.kvindex.Tombstone(key, idxRev.Revision)
	if err != nil {
		tw.storeTxnCommon.s.lg.Fatal(
			"failed to tombstone an existing key",
			zap.String("key", string(key)),
			zap.Error(err),
		)
	}
	tw.changes = append(tw.changes, kv)

	item := lease.LeaseItem{Key: string(key)}
	leaseID := tw.s.le.GetLease(item)

	if leaseID != lease.NoLease {
		err = tw.s.le.Detach(leaseID, []lease.LeaseItem{item})
		if err != nil {
			tw.storeTxnCommon.s.lg.Error(
				"failed to detach old lease from a key",
				zap.Error(err),
			)
		}
	}
}

func (tw *storeTxnWrite) kvDelete(key []byte) {

	tw.tx.UnsafeKvDeleteKey(tw.beginRev+1, int64(len(tw.changes)), key)
	tw.s.pgKvIndex.Tombstone(key, tw.beginRev+1)
	kv := mvccpb.KeyValue{Key: key}

	tw.changes = append(tw.changes, kv)

	item := lease.LeaseItem{Key: string(key)}
	leaseID := tw.s.le.GetLease(item)

	if leaseID != lease.NoLease {
		err := tw.s.le.Detach(leaseID, []lease.LeaseItem{item})
		if err != nil {
			tw.storeTxnCommon.s.lg.Error(
				"failed to detach old lease from a key",
				zap.Error(err),
			)
		}
	}
}

func (tw *storeTxnWrite) Changes() []mvccpb.KeyValue { return tw.changes }
