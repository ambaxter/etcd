package backend_test

import (
	"testing"

	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestPgKvBuffer(t *testing.T) {
	if !backend.TEST_POSTGRES {
		t.Skip("Skipping Postgres backend test")
	}
	l := zaptest.NewLogger(t)
	d := backend.PgKvBuffer[*backend.PgKvLogEntry]{}
	d.Put([]byte("key01"), 1, &backend.PgKvLogEntry{
		RevMain:   1,
		RevSub:    1,
		RevCreate: 1,
		Lease:     1,
		Version:   1,
		Key:       []byte("key01"),
		Value:     []byte("key01"),
	})
	d.Put([]byte("key02"), 1, &backend.PgKvLogEntry{
		RevMain:   1,
		RevSub:    2,
		RevCreate: 1,
		Lease:     1,
		Version:   1,
		Key:       []byte("key02"),
		Value:     []byte("key02"),
	})
	d.Put([]byte("key03"), 1, &backend.PgKvLogEntry{
		RevMain:   1,
		RevSub:    3,
		RevCreate: 1,
		Lease:     1,
		Version:   1,
		Key:       []byte("key03"),
		Value:     []byte("key03"),
	})
	d.Put([]byte("key01"), 2, &backend.PgKvLogEntry{
		RevMain:   2,
		RevSub:    1,
		RevCreate: 1,
		Lease:     1,
		Version:   2,
		Key:       []byte("key01"),
		Value:     []byte("up02"),
	})
	d.Put([]byte("key02"), 2, &backend.PgKvLogEntry{
		RevMain:   2,
		RevSub:    2,
		RevCreate: 1,
		Lease:     1,
		Version:   2,
		Key:       []byte("key02"),
		Value:     []byte("up02"),
	})

	i := d.LogRangeEntries([]byte("key02"), []byte{}, 2)
	for i.Next() {
		entry := i.Value()
		l.Info("EEE", zap.Int64("main", entry.RevMain))

	}
}

func TestPgKvStoreIndex(t *testing.T) {
	if !backend.TEST_POSTGRES {
		t.Skip("Skipping Postgres backend test")
	}
	l := zaptest.NewLogger(t)
	index := backend.PgKvStoreIndex{}
	index.Put([]byte("key01"), 1, 1, 1)
	index.Put([]byte("key02"), 1, 1, 1)
	index.Put([]byte("key01"), 2, 2, 2)
	index.Put([]byte("key02"), 3, 3, 2)
	c, v, ok := index.Get([]byte("key03"), 0)
	if ok {
		l.Info("blablabl", zap.Int64("c", c), zap.Int64("v", v))
	} else {
		l.Info("missing")
	}

	cnt := index.CountRevisions([]byte("key01"), []byte("key02"), 1)
	l.Info("count", zap.Int("count", cnt))
	str := index.KeyRange([]byte("key01"), []byte{}, 1)
	l.Info("string", zap.Int("len", len(str)))
	index.Tombstone([]byte("key02"), 4)
	index.Compact(5)
	c, v, ok = index.Get([]byte("key02"), 1)
	if ok {
		l.Info("blablabl", zap.Int64("c", c), zap.Int64("v", v))
	} else {
		l.Info("missing")
	}
}
