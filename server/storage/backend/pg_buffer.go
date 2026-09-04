package backend

import (
	"math"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/tidwall/btree"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
)

type KeyIter[K any] interface {
	Next() bool
	Key() K
}

type MapKeyIter[K, U any] struct {
	map_fn func(K) U
	iter   KeyIter[K]
}

func (m *MapKeyIter[K, U]) Next() bool {
	return m.iter.Next()
}

func (m *MapKeyIter[K, U]) Key() U {
	key := m.iter.Key()
	return m.map_fn(key)
}

func MapKey[K, U any](iter KeyIter[K], map_fn func(K) U) KeyIter[U] {
	return &MapKeyIter[K, U]{
		map_fn: map_fn,
		iter:   iter,
	}
}

func KeyFilter[K any](iter KeyIter[K], check_fn func(K) bool) KeyIter[K] {
	return &KeyFilterIter[K]{
		check_fn: check_fn,
		iter:     iter,
	}
}

type KeyFilterIter[K any] struct {
	check_fn func(K) bool
	iter     KeyIter[K]
}

func (m *KeyFilterIter[K]) Next() bool {
	for m.iter.Next() {
		if !m.check_fn(m.iter.Key()) {
			return true
		}
	}
	return false
}

func (m *KeyFilterIter[K]) Key() K {
	return m.iter.Key()
}

type KVIter[K, V any] interface {
	Next() bool
	Key() K
	Value() V
}

type FilterKvIter[K, V any] struct {
	check_fn func(V) bool
	iter     KVIter[K, V]
}

func FilterKv[K, V any](iter KVIter[K, V], check_fn func(V) bool) KVIter[K, V] {
	return &FilterKvIter[K, V]{
		check_fn: check_fn,
		iter:     iter,
	}
}

func (m *FilterKvIter[K, V]) Next() bool {
	for m.iter.Next() {
		value := m.iter.Value()
		pass := m.check_fn(value)
		if pass {
			return true
		}
	}
	return false
}

func (m *FilterKvIter[K, V]) Key() K {
	return m.iter.Key()
}

func (m *FilterKvIter[K, V]) Value() V {
	return m.iter.Value()
}

type MapKvIter[K, V, U any] struct {
	map_fn func(V) U
	iter   KVIter[K, V]
}

func MapKv[K, V, U any](iter KVIter[K, V], map_fn func(V) U) KVIter[K, U] {
	return &MapKvIter[K, V, U]{
		map_fn: map_fn,
		iter:   iter,
	}
}

func (m *MapKvIter[K, V, U]) Next() bool {
	return m.iter.Next()
}

func (m *MapKvIter[K, V, U]) Key() K {
	return m.iter.Key()
}

func (m *MapKvIter[K, V, U]) Value() U {
	return m.map_fn(m.iter.Value())
}

type KeysBufferIter[K, V any] struct {
	iter KVIter[K, V]
}

func (m *KeysBufferIter[K, V]) Next() bool {
	return m.iter.Next()
}

func (m *KeysBufferIter[K, V]) Key() K {
	return m.iter.Key()
}

type PgKvBuffer[V any] struct {
	m btree.Map[string, *btree.Map[int64, V]]
}

func (d *PgKvBuffer[V]) getMutKeyEntries(key string) *btree.Map[int64, V] {
	tree, ok := d.m.GetMut(key)
	if !ok {
		tree = new(btree.Map[int64, V])
		d.m.Set(key, tree)
	}
	return tree
}

func (d *PgKvBuffer[V]) Put(key []byte, revMain int64, entry V) {
	sKey := unsafe.String(unsafe.SliceData(key), len(key))
	keyEntries := d.getMutKeyEntries(sKey)
	keyEntries.Set(revMain, entry)
}

func (d *PgKvBuffer[V]) GetAtNow(key []byte) (V, bool) {
	sKey := unsafe.String(unsafe.SliceData(key), len(key))
	var entry V
	tree, ok := d.m.Get(sKey)
	if ok {
		_, entry, _ = tree.Max()
		return entry, true
	} else {
		return entry, false
	}
}

func (d *PgKvBuffer[V]) GetAtRev(key []byte, latestRev int64) (V, bool) {
	sKey := unsafe.String(unsafe.SliceData(key), len(key))
	tree, ok := d.m.Get(sKey)
	var entry V
	if ok {
		if latestRev == 0 {
			latestRev = math.MaxInt64
		}
		hasEntry := false
		hasPtr := &hasEntry
		entryPtr := &entry
		tree.Descend(
			latestRev,
			func(key int64, value V) bool {
				(*hasPtr) = true
				(*entryPtr) = value
				return false
			},
		)
		return entry, hasEntry
	} else {
		return entry, false
	}
}

func (d *PgKvBuffer[V]) Copy() PgKvBuffer[V] {
	return PgKvBuffer[V]{m: *d.m.Copy()}
}

func (d *PgKvBuffer[V]) Clear() {
	d.m.Clear()
}

type KvRangeIter[V any] struct {
	iter        btree.MapIter[string, *btree.Map[int64, V]]
	key, endKey string
	started     bool
	singleEntry bool
}

func (i *KvRangeIter[V]) Next() bool {
	if i.started {
		if i.singleEntry {
			return false
		}
		if i.iter.Next() {
			if len(i.endKey) == 0 || i.iter.Key() < i.endKey {
				return true
			}
		}
	} else {
		i.started = true
		if i.iter.Seek(i.key) {
			if len(i.endKey) == 0 || i.iter.Key() < i.endKey {
				return true
			}
		}
	}
	return false
}

func (i *KvRangeIter[V]) Key() string {
	return i.iter.Key()
}

func (i *KvRangeIter[V]) Value() *btree.Map[int64, V] {
	return i.iter.Value()
}

func (d *PgKvBuffer[V]) RangeEntries(key, endKey []byte) KVIter[string, V] {
	return d.rangeMaxRevEntries(key, endKey)
}

func (d *PgKvBuffer[V]) LogRangeEntries(key, endKey []byte, latestRev int64) KVIter[string, V] {
	return d.rangeLogByRevEntries(key, endKey, latestRev)
}

func (d *PgKvBuffer[V]) rangeMaxRevEntries(key, endKey []byte) KVIter[string, V] {
	sKey := unsafe.String(unsafe.SliceData(key), len(key))
	var singleEntry bool
	var sEndKey string
	if endKey == nil {
		singleEntry = true
	} else {
		sEndKey = unsafe.String(unsafe.SliceData(endKey), len(endKey))
	}
	return MapKv(
		&KvRangeIter[V]{iter: d.m.Iter(), key: sKey, endKey: sEndKey, singleEntry: singleEntry},
		func(entry *btree.Map[int64, V]) V {
			_, max, _ := entry.Max()
			return max
		},
	)
}

func (d *PgKvBuffer[V]) rangeLogByRevEntries(key, endKey []byte, latestRev int64) KVIter[string, V] {
	sKey := unsafe.String(unsafe.SliceData(key), len(key))
	var singleEntry bool
	var sEndKey string
	if endKey == nil {
		singleEntry = true
	} else {
		sEndKey = unsafe.String(unsafe.SliceData(endKey), len(endKey))
	}
	if latestRev == 0 {
		latestRev = math.MaxInt64
	}
	f := FilterKv(
		&KvRangeIter[V]{iter: d.m.Iter(), key: sKey, endKey: sEndKey, singleEntry: singleEntry},
		func(entry *btree.Map[int64, V]) bool {
			pass := false
			pPass := &pass
			entry.Descend(
				latestRev,
				func(key int64, value V) bool {
					(*pPass) = true
					return false
				},
			)
			return pass
		},
	)
	return MapKv(
		f,
		func(entry *btree.Map[int64, V]) V {
			var value V
			valPtr := &value
			entry.Descend(
				latestRev,
				func(key int64, value V) bool {
					(*valPtr) = value
					return false
				},
			)
			return value
		},
	)
}

type RowRangeIter struct {
	lg   *zap.Logger
	kv   mvccpb.KeyValue
	rows pgx.Rows
}

func newRowRangeIter(lg *zap.Logger, rows pgx.Rows) RowRangeIter {
	return RowRangeIter{
		lg:   lg,
		rows: rows,
	}
}

func (i *RowRangeIter) Next() bool {
	next := i.rows.Next()
	if next {
		var lease zeronull.Int8
		err := i.rows.Scan(
			&i.kv.CreateRevision,
			&i.kv.ModRevision,
			&lease, &i.kv.Version,
			&i.kv.Key,
			&i.kv.Value)
		i.kv.Lease = int64(lease)
		if err != nil {
			i.lg.Panic("Error during scan", zap.Error(err))
		}
	}
	return next
}

func (i *RowRangeIter) Key() string {
	key := i.kv.Key
	return unsafe.String(unsafe.SliceData(key), len(key))
}

func (i *RowRangeIter) Value() mvccpb.KeyValue {
	return i.kv
}

type RowRangeKeyIter struct {
	lg   *zap.Logger
	key  []byte
	rows pgx.Rows
}

func newRowRangeKeyIter(lg *zap.Logger, rows pgx.Rows) RowRangeKeyIter {
	return RowRangeKeyIter{
		lg:   lg,
		rows: rows,
	}
}

func (i *RowRangeKeyIter) Next() bool {
	next := i.rows.Next()
	if next {
		err := i.rows.Scan(&i.key)
		if err != nil {
			i.lg.Panic("Error during scan", zap.Error(err))
		}
	}
	return next
}

func (i *RowRangeKeyIter) Key() string {
	return unsafe.String(unsafe.SliceData(i.key), len(i.key))
}

type PgKvStoreIndex struct {
	d PgKvBuffer[KvVersion]
}

func (p *PgKvStoreIndex) Get(key []byte, atRev int64) (created, version int64, ok bool) {
	return p.unsafeGet(key, atRev)
}

func (p *PgKvStoreIndex) Put(key []byte, atRev, created, version int64) {
	p.d.Put(key, atRev, KvVersion{CreateRevision: created, Version: version})
}

func (p *PgKvStoreIndex) Tombstone(key []byte, atRev int64) {
	p.d.Put(key, atRev, KvVersion{})
}

func (p *PgKvStoreIndex) unsafeGet(key []byte, atRev int64) (created, version int64, ok bool) {
	kvVersion, ok := p.d.GetAtRev(key, atRev)
	if ok && !kvVersion.isTombstone() {
		return kvVersion.CreateRevision, kvVersion.Version, true
	} else {
		return 0, 0, false
	}
}

func (p *PgKvStoreIndex) CountRevisions(key, end []byte, atRev int64) int {
	total := 0
	i := FilterKv(
		p.d.rangeLogByRevEntries(key, end, atRev),
		func(entry KvVersion) bool { return !entry.isTombstone() })
	for i.Next() {
		total += 1
	}
	return total
}

func (p *PgKvStoreIndex) KeyRange(key, end []byte, atRev int64) [][]byte {
	i := MapKey(
		FilterKv(
			p.d.rangeLogByRevEntries(key, end, atRev),
			func(entry KvVersion) bool { return !entry.isTombstone() }),
		func(key string) []byte { return []byte(key) },
	)
	var keys [][]byte
	for i.Next() {
		keys = append(keys, i.Key())
	}
	return keys
}

func (p *PgKvStoreIndex) Compact(atRev int64) {
	clone := p.d.m.Copy()
	i := clone.Iter()
	for i.Next() {
		key := i.Key()
		revs, _ := p.d.m.GetMut(key)
		_, max, ok := revs.Max()
		if !ok || max.isTombstone() {
			p.d.m.Delete(key)
		} else {
			for {
				if revs.Len() > 1 {
					revs.PopMin()
				} else {
					break
				}
			}
		}
	}
}
