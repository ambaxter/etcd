package backend_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/storage/backend"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestPgBatchTxPut(t *testing.T) {
	if !backend.TEST_POSTGRES {
		t.Skip("Skipping Postgres backend test")
	}
	for _, kv := range backend.PgBackendTypes {
		testName := fmt.Sprintf("%s-%s", t.Name(), kv)
		t.Run(testName, func(t *testing.T) {
			b, _ := betesting.NewTmpPgBackend(t, time.Hour, 10000, kv)

			defer betesting.Close(t, b)

			tx := b.BatchTx()

			tx.Lock()

			// create bucket
			tx.UnsafeCreateBucket(schema.Test)

			// put
			v := []byte("bar")
			tx.UnsafePut(schema.Test, []byte("foo"), v)

			tx.Unlock()

			// check put result before and after tx is committed
			for k := 0; k < 2; k++ {
				tx.Lock()
				_, gv := tx.UnsafeRange(schema.Test, []byte("foo"), nil, 0)
				tx.Unlock()
				if !reflect.DeepEqual(gv[0], v) {
					t.Errorf("v = %s, want %s", gv[0], v)
				}
				tx.Commit()
			}
		})
	}
}

func TestPgBatchTxRange(t *testing.T) {
	if !backend.TEST_POSTGRES {
		t.Skip("Skipping Postgres backend test")
	}
	for _, kv := range backend.PgBackendTypes {
		testName := fmt.Sprintf("%s-%s", t.Name(), kv)
		t.Run(testName, func(t *testing.T) {
			b, _ := betesting.NewTmpPgBackend(t, time.Hour, 10000, kv)

			defer betesting.Close(t, b)

			tx := b.BatchTx()
			tx.Lock()
			defer tx.Unlock()

			tx.UnsafeCreateBucket(schema.Test)
			// put keys
			allKeys := [][]byte{[]byte("foo"), []byte("foo1"), []byte("foo2")}
			allVals := [][]byte{[]byte("bar"), []byte("bar1"), []byte("bar2")}
			for i := range allKeys {
				tx.UnsafePut(schema.Test, allKeys[i], allVals[i])
			}

			tests := []struct {
				key    []byte
				endKey []byte
				limit  int64

				wkeys [][]byte
				wvals [][]byte
			}{
				// single key
				{
					[]byte("foo"), nil, 0,
					allKeys[:1], allVals[:1],
				},
				// single key, bad
				{
					[]byte("doo"), nil, 0,
					nil, nil,
				},
				// key range
				{
					[]byte("foo"), []byte("foo1"), 0,
					allKeys[:1], allVals[:1],
				},
				// key range, get all keys
				{
					[]byte("foo"), []byte("foo3"), 0,
					allKeys, allVals,
				},
				// key range, bad
				{
					[]byte("goo"), []byte("goo3"), 0,
					nil, nil,
				},
				// key range with effective limit
				{
					[]byte("foo"), []byte("foo3"), 1,
					allKeys[:1], allVals[:1],
				},
				// key range with limit
				{
					[]byte("foo"), []byte("foo3"), 4,
					allKeys, allVals,
				},
			}
			for i, tt := range tests {
				keys, vals := tx.UnsafeRange(schema.Test, tt.key, tt.endKey, tt.limit)
				if !reflect.DeepEqual(keys, tt.wkeys) {
					t.Errorf("#%d: keys = %+v, want %+v", i, keys, tt.wkeys)
				}
				if !reflect.DeepEqual(vals, tt.wvals) {
					t.Errorf("#%d: vals = %+v, want %+v", i, vals, tt.wvals)
				}
			}
		})
	}
}

func TestKvBatchQuery(t *testing.T) {
	if !backend.TEST_POSTGRES {
		t.Skip("Skipping Postgres backend test")
	}
	for _, kv := range backend.PgKvBackendTypes {
		testName := fmt.Sprintf("%s-%s", t.Name(), kv)
		t.Run(testName, func(t *testing.T) {
			b, _ := betesting.NewTmpPgBackend(t, time.Hour, 10000, kv)

			defer betesting.Close(t, b)
			l := zaptest.NewLogger(t)
			tx := b.BatchTx()

			// TODO: Spot check looks ok. Need actually testing, tho
			tx.Lock()
			tx.UnsafeKvPutKey(1, 1, 1, 1, 1, []byte("key01"), []byte("value"))
			tx.UnsafeKvPutKey(1, 2, 1, 2, 1, []byte("key02"), []byte("value"))
			tx.UnsafeKvPutKey(1, 3, 1, 3, 1, []byte("key03"), []byte("value"))
			tx.UnsafeKvPutKey(1, 4, 1, 4, 1, []byte("key04"), []byte("value"))
			kvs := tx.UnsafeKvRangeEntries([]byte("key02"), []byte{}, 0, backend.KvRangeOptions{})
			l.Info("KV", zap.Int("len", len(kvs)))
			tx.Unlock()
			rtx := b.ConcurrentReadTx()
			rtx.RLock()
			kvs = rtx.UnsafeKvRangeEntries([]byte("key02"), []byte{}, 0, backend.KvRangeOptions{})
			l.Info("KV", zap.Int("len", len(kvs)))
			rtx.RUnlock()
			tx.Lock()
			tx.UnsafeKvPutKey(2, 1, 2, 5, 1, []byte("key05"), []byte("value2"))
			tx.UnsafeKvPutKey(2, 2, 1, 2, 2, []byte("key02"), []byte("value2"))
			tx.UnsafeKvPutKey(2, 3, 1, 3, 2, []byte("key03"), []byte("value2"))
			tx.UnsafeKvPutKey(2, 4, 0, 1, 0, []byte("key01"), nil)
			kvs = tx.UnsafeKvRangeEntries([]byte(""), []byte{}, 0, backend.KvRangeOptions{})
			l.Info("KV", zap.Int("len", len(kvs)))
			kvs = tx.UnsafeKvLogRangeEntries([]byte(""), []byte{}, 1, 0, backend.KvRangeOptions{})
			l.Info("KV", zap.Int("len", len(kvs)))
			tx.Unlock()
			tx.Commit()
			tx.LockKvCompact()
			l.Info("compacting now")
			compactedCount, err := tx.UnsafeKvLogCompact(1, func(entry mvccpb.KeyValue) error {
				l.Info("compacted",
					zap.Int64("rev_create", entry.CreateRevision),
					zap.Int64("rev_mod", entry.ModRevision),
					zap.Int64("lease", entry.Lease),
					zap.Int64("version", entry.Version),
					zap.Binary("key", entry.Key),
					zap.Binary("value", entry.Value),
				)
				return nil
			})
			if err != nil {
				t.Errorf("compaction error = %v, want nil", err)
			}
			if compactedCount != 3 {
				t.Errorf("compactedCount = %d, want 3", compactedCount)
			}
			l.Info("finished compacting")
			tx.UnlockKvCompact()
			rtx = b.ConcurrentReadTx()
			rtx.RLock()
			kvs = rtx.UnsafeKvLogRangeEntries([]byte(""), []byte{}, 2, 0, backend.KvRangeOptions{})
			l.Info("KV", zap.Int("len", len(kvs)))
			rtx.RUnlock()
		})
	}
}
