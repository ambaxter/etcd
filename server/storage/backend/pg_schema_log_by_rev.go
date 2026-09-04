package backend

import (
	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/dialect"
	"github.com/stephenafamo/bob/dialect/psql/dm"
	"github.com/stephenafamo/bob/dialect/psql/im"
	"github.com/stephenafamo/bob/dialect/psql/sm"
)

var (
	// Log Ordered By Revision (LBR)
	kvLbrTblDef string = `CREATE TABLE IF NOT EXISTS kv_lbr(
	rev_main bigint NOT NULL,
	rev_sub bigint NOT NULL,
	rev_create bigint,
	lease bigint,
	version bigint,
	key bytea,
	value bytea,
	PRIMARY KEY (rev_main, rev_sub)
)`

	// To speed up log range key searches
	kvLbrKeyRevIdxDef string = `CREATE UNIQUE INDEX IF NOT EXISTS kv_lbr_key_rev_idx ON kv_lbr(
	key ASC, rev_main DESC
)`
)

// by-rev-shared

var (
	kvLbrFilterKeysAndRev bob.BaseQuery[*dialect.SelectQuery] = psql.Select(
		sm.Distinct("key"),
		sm.Columns("key", "rev_main", "rev_sub", "rev_create", "lease", "version", "value"),
		sm.From(
			psql.Select(
				sm.Columns("key", "rev_main", "rev_sub", "rev_create", "lease", "version", "value"),
				sm.From("kv_lbr"),
				sm.Where(
					psql.And(
						wKeyRangeFilter,
						psql.Raw("latest_rev").IsNull().Or(psql.Raw("rev_main").LTE(psql.Raw("latest_rev"))),
					),
				),
			),
		),
		sm.OrderBy("key").Asc(),
		sm.OrderBy("rev_main").Desc(),
		sm.OrderBy("rev_sub").Desc(),
	)

	KV_LBR_RANGE_ENTRY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_range_entry",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "latest_rev bigint", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("rev_create", "rev_main AS rev_mod", "lease", "version", "key", "value"),
			sm.From(kvLbrFilterKeysAndRev),
			sm.Where(wRevModCreateFilter.And(psql.Raw("value").IsNotNull())),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_RANGE_KEY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_range_key",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "latest_rev bigint", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"key bytea"},
		queryDef: psql.Select(
			sm.Columns("key"),
			sm.From(kvLbrFilterKeysAndRev),
			sm.Where(wRevModCreateFilter.And(psql.Raw("value").IsNotNull())),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_RANGE_COUNT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_range_count",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "latest_rev bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"count bigint"},
		queryDef: psql.Select(
			sm.Columns("count(*)"),
			sm.From(kvLbrFilterKeysAndRev),
			sm.Where(wRevModCreateFilter.And(psql.Raw("value").IsNotNull())),
		),
	})

	KV_LBR_ENTRY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_entry",
		FnParam:     []string{"latest_rev bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("rev_create", "rev_main AS rev_mod", "lease", "version", "key", "value"),
			sm.From("kv_lbr"),
			sm.Where(
				psql.And(
					psql.Raw("key").IsNotNull(),
					psql.Raw("latest_rev").IsNull().Or(psql.Raw("rev_main").LTE(psql.Raw("latest_rev"))),
				),
			),
		),
	})
)

// by-rev-now-norm

var (
	// Most recent keys
	kvLbrNowNormTblDef string = `CREATE TABLE IF NOT EXISTS kv_lbr_now_norm(
	rev_main bigint NOT NULL,
	rev_sub bigint NOT NULL,
	key bytea NOT NULL,
	PRIMARY KEY (key)
)`

	// To speed up compaction
	kvLbrNowNormRevIdxDef string = `CREATE UNIQUE INDEX IF NOT EXISTS kv_lbr_now_norm_rev_idx ON kv_lbr_now_norm(
	rev_main, rev_sub
)`

	sLbrNowNormEntries bob.BaseQuery[*dialect.SelectQuery] = psql.Select(
		sm.Columns("rev_main", "rev_sub", "rev_create", "lease", "version", "now.key", "value"),
		sm.From("kv_lbr_now_norm").As("now"),
		sm.InnerJoin("kv_lbr").Using("rev_main", "rev_sub"),
	)

	KV_LBR_NOW_NORM_RANGE_ENTRY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_now_norm_range_entry",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("rev_create", "rev_main AS rev_mod", "lease", "version", "key", "value"),
			sm.From(sLbrNowNormEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
			sm.OrderBy("key").Asc(),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_NOW_NORM_RANGE_KEY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_now_norm_range_key",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"key bytea"},
		queryDef: psql.Select(
			sm.Columns("key"),
			sm.From(sLbrNowNormEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
			sm.OrderBy("key").Asc(),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_NOW_NORM_RANGE_COUNT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_now_norm_range_count",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"count bigint"},
		queryDef: psql.Select(
			sm.Columns("count(*)"),
			sm.From(sLbrNowNormEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
		),
	})

	KV_LBR_NOW_NORM_DB_SIZE pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_now_norm_db_size",
		FnTableCols: []string{"size bigint"},
		queryDef: psql.Select(
			sm.Columns(
				psql.Raw("pg_total_relation_size('buckets'::regclass)").Plus(
					psql.Raw("pg_total_relation_size('bucket_data'::regclass)"),
				).Plus(
					psql.Raw("pg_total_relation_size('kv_lbr'::regclass)"),
				).Plus(
					psql.Raw("pg_total_relation_size('kv_lbr_now_norm'::regclass)"),
				),
			),
		),
	})

	KV_LBR_NOW_NORM_UPSERT pgFn[*dialect.InsertQuery] = newPgFn(pgFunctionDef[*dialect.InsertQuery]{
		FnName:  "fn_kv_lbr_now_norm_upsert",
		FnParam: []string{"rev_mains bigint[]", "rev_subs bigint[]", "rev_creates bigint[]", "leases bigint[]", "versions bigint[]", "keys bytea[]", "_values bytea[]"},
		queryDef: psql.Insert(
			im.With("log_upsert", "rev_main", "rev_sub", "key").As(
				psql.Insert(
					im.Into("kv_lbr", "rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
					im.Query(
						psql.Select(
							sm.Columns("rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
							sm.From(psql.Raw("unnest(rev_mains, rev_subs, rev_creates, leases, versions, keys, _values)")).As("u", "rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
						),
					),
					im.OnConflict("rev_main", "rev_sub").DoUpdate(
						im.SetExcluded("rev_create"),
						im.SetExcluded("version"),
						im.SetExcluded("lease"),
						im.SetExcluded("key"),
						im.SetExcluded("value"),
					),
					im.Returning("rev_main", "rev_sub", "key"),
				),
			),
			im.Into("kv_lbr_now_norm", "key", "rev_main", "rev_sub"),
			im.Query(
				psql.Select(
					sm.Distinct("key"),
					sm.Columns("key", "rev_main", "rev_sub"),
					sm.From("log_upsert"),
					sm.Where(psql.Raw("key").IsNotNull()),
					sm.OrderBy("key").Asc(),
					sm.OrderBy("rev_main").Desc(),
					sm.OrderBy("rev_sub").Desc(),
				),
			),
			im.OnConflict("key").DoUpdate(
				im.SetExcluded("rev_main"),
				im.SetExcluded("rev_sub"),
			),
		),
	})

	KV_LBR_NOW_NORM_COMPACT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_now_norm_compact",
		FnParam:     []string{"compact_rev bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.With("delete_batch").As(psql.Delete(
				dm.FromAs("kv_lbr", "l"),
				dm.Where(
					psql.And(
						psql.Raw("compact_rev").IsNull().Or(psql.Raw("l.rev_main").LTE(psql.Raw("compact_rev"))),
						psql.RawQuery("NOT EXISTS (SELECT 1 FROM kv_lbr_now_norm AS now WHERE l.rev_main = now.rev_main AND l.rev_sub = now.rev_sub)"),
					),
				),
				dm.Returning(
					psql.Raw("l.rev_main"),
					psql.Raw("l.rev_sub"),
					psql.Raw("l.rev_create"),
					psql.Raw("l.lease"),
					psql.Raw("l.version"),
					psql.Raw("l.key"),
					psql.Raw("l.value"),
				),
			)),
			sm.Columns("rev_create", "rev_main", "lease", "version", "key", "value"),
			sm.From("delete_batch").As("d", "rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
			sm.OrderBy("rev_main").Asc(),
			sm.OrderBy("rev_sub").Asc(),
		),
	})
)

func kvLbrNowNormSchemaBundle(useOriole bool) pgSchemaBundle {
	return pgSchemaBundle{
		tableDefs: []pgTable{
			t(kvLbrTblDef, "kv_lbr", useOriole, -1, -1, -1, -1),
			t(kvLbrNowNormTblDef, "kv_lbr_now_norm", useOriole, -1, -1, -1, -1),
		},
		indexDefs: []pgIndex{
			i(kvLbrKeyRevIdxDef, "kv_lbr_key_rev_idx", useOriole, -1, -1),
			i(kvLbrNowNormRevIdxDef, "kv_lbr_now_norm_rev_idx", useOriole, -1, -1),
		},
		selectFns: []pgFn[*dialect.SelectQuery]{
			KV_LBR_NOW_NORM_RANGE_ENTRY,
			KV_LBR_NOW_NORM_RANGE_KEY,
			KV_LBR_NOW_NORM_RANGE_COUNT,
			KV_LBR_RANGE_ENTRY,
			KV_LBR_RANGE_KEY,
			KV_LBR_RANGE_COUNT,
			KV_LBR_NOW_NORM_DB_SIZE,
		},
		insertFns: []pgFn[*dialect.InsertQuery]{
			KV_LBR_NOW_NORM_UPSERT,
		},
		deleteFns: []pgFn[*dialect.DeleteQuery]{},
		volSelectFns: []pgFn[*dialect.SelectQuery]{
			KV_LBR_NOW_NORM_COMPACT,
		},
	}
}

// by-rev-now-nifs

var (
	// Upcoming log entries, used for shared queries. Normalization is for sissies
	kvLbrNowNifsTblDef string = `CREATE TABLE IF NOT EXISTS kv_lbr_now_nifs(
	rev_main bigint NOT NULL,
	rev_sub bigint NOT NULL,
	rev_create bigint,
	lease bigint,
	version bigint,
	key bytea,
	value bytea,
	PRIMARY KEY (key)
)`

	// To speed up compaction
	kvLbrNowNifsRevIdxDef string = `CREATE UNIQUE INDEX IF NOT EXISTS kv_lbr_now_nifs_rev_idx ON kv_lbr_now_nifs(
	rev_main, rev_sub
)`

	sLbrNowNifsEntries bob.BaseQuery[*dialect.SelectQuery] = psql.Select(
		sm.Columns("rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
		sm.From("kv_lbr_now_nifs"),
	)

	KV_LBR_NOW_NIFS_RANGE_ENTRY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_now_nifs_range_entry",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("rev_create", "rev_main AS rev_mod", "lease", "version", "key", "value"),
			sm.From(sLbrNowNifsEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
			sm.OrderBy("key").Asc(),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_NOW_NIFS_RANGE_KEY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_now_nifs_range_key",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"key bytea"},
		queryDef: psql.Select(
			sm.Columns("key"),
			sm.From(sLbrNowNifsEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
			sm.OrderBy("key").Asc(),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_NOW_NIFS_RANGE_COUNT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_now_nifs_range_count",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"count bigint"},
		queryDef: psql.Select(
			sm.Columns("count(*)"),
			sm.From(sLbrNowNifsEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
		),
	})

	KV_LBR_NOW_NIFS_UPSERT pgFn[*dialect.InsertQuery] = newPgFn(pgFunctionDef[*dialect.InsertQuery]{
		FnName:  "fn_kv_lbr_now_nifs_upsert",
		FnParam: []string{"rev_mains bigint[]", "rev_subs bigint[]", "rev_creates bigint[]", "leases bigint[]", "versions bigint[]", "keys bytea[]", "_values bytea[]"},
		queryDef: psql.Insert(
			im.With("log_upsert", "rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value").As(
				psql.Insert(
					im.Into("kv_lbr", "rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
					im.Query(
						psql.Select(
							sm.Columns("rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
							sm.From(psql.Raw("unnest(rev_mains, rev_subs, rev_creates, leases, versions, keys, _values)")).As("u", "rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
						),
					),
					im.OnConflict("rev_main", "rev_sub").DoUpdate(
						im.SetExcluded("rev_create"),
						im.SetExcluded("version"),
						im.SetExcluded("lease"),
						im.SetExcluded("key"),
						im.SetExcluded("value"),
					),
					im.Returning("rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
				),
			),
			im.Into("kv_lbr_now_nifs", "key", "rev_main", "rev_sub", "rev_create", "lease", "version", "value"),
			im.Query(
				psql.Select(
					sm.Distinct("key"),
					sm.Columns("key", "rev_main", "rev_sub", "rev_create", "lease", "version", "value"),
					sm.From("log_upsert"),
					sm.Where(psql.Raw("key").IsNotNull()),
					sm.OrderBy("key").Asc(),
					sm.OrderBy("rev_main").Desc(),
					sm.OrderBy("rev_sub").Desc(),
				),
			),
			im.OnConflict("key").DoUpdate(
				im.SetExcluded("rev_main"),
				im.SetExcluded("rev_sub"),
				im.SetExcluded("rev_create"),
				im.SetExcluded("version"),
				im.SetExcluded("lease"),
				im.SetExcluded("value"),
			),
		),
	})

	KV_LBR_NOW_NIFS_COMPACT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_now_nifs_compact",
		FnParam:     []string{"compact_rev bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.With("delete_batch").As(psql.Delete(
				dm.FromAs("kv_lbr", "l"),
				dm.Where(
					psql.And(
						psql.Raw("compact_rev").IsNull().Or(psql.Raw("l.rev_main").LTE(psql.Raw("compact_rev"))),
						psql.RawQuery("NOT EXISTS (SELECT 1 FROM kv_lbr_now_nifs AS now WHERE l.rev_main = now.rev_main AND l.rev_sub = now.rev_sub)"),
					),
				),
				dm.Returning(
					psql.Raw("l.rev_main"),
					psql.Raw("l.rev_sub"),
					psql.Raw("l.rev_create"),
					psql.Raw("l.lease"),
					psql.Raw("l.version"),
					psql.Raw("l.key"),
					psql.Raw("l.value"),
				),
			)),
			sm.Columns("rev_create", "rev_main", "lease", "version", "key", "value"),
			sm.From("delete_batch").As("d", "rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
			sm.OrderBy("rev_main").Asc(),
			sm.OrderBy("rev_sub").Asc(),
		),
	})

	KV_LBR_NOW_NIFS_DB_SIZE pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_now_nifs_db_size",
		FnTableCols: []string{"size bigint"},
		queryDef: psql.Select(
			sm.Columns(
				psql.Raw("pg_total_relation_size('buckets'::regclass)").Plus(
					psql.Raw("pg_total_relation_size('bucket_data'::regclass)"),
				).Plus(
					psql.Raw("pg_total_relation_size('kv_lbr'::regclass)"),
				).Plus(
					psql.Raw("pg_total_relation_size('kv_lbr_now_nifs'::regclass)"),
				),
			),
		),
	})
)

func kvLbrNowNifsSchemaBundle(useOriole bool) pgSchemaBundle {
	return pgSchemaBundle{
		tableDefs: []pgTable{
			t(kvLbrTblDef, "kv_lbr", useOriole, -1, -1, -1, -1),
			t(kvLbrNowNifsTblDef, "kv_lbr_now_nifs", useOriole, -1, -1, -1, -1),
		},
		indexDefs: []pgIndex{
			i(kvLbrKeyRevIdxDef, "kv_lbr_key_rev_idx", useOriole, -1, -1),
			i(kvLbrNowNifsRevIdxDef, "kv_lbr_now_nifs_rev_idx", useOriole, -1, -1),
		},
		selectFns: []pgFn[*dialect.SelectQuery]{
			KV_LBR_NOW_NIFS_RANGE_ENTRY,
			KV_LBR_NOW_NIFS_RANGE_KEY,
			KV_LBR_NOW_NIFS_RANGE_COUNT,
			KV_LBR_RANGE_ENTRY,
			KV_LBR_RANGE_KEY,
			KV_LBR_RANGE_COUNT,
			KV_LBR_NOW_NIFS_DB_SIZE,
		},
		insertFns: []pgFn[*dialect.InsertQuery]{
			KV_LBR_NOW_NIFS_UPSERT,
		},
		deleteFns: []pgFn[*dialect.DeleteQuery]{},
		volSelectFns: []pgFn[*dialect.SelectQuery]{
			KV_LBR_NOW_NIFS_COMPACT,
		},
	}
}

var (
	// Log Ordered By Revision (LBR) using Key Id
	// Crazy town
	kvKeysTblDef string = `CREATE TABLE IF NOT EXISTS kv_keys(
	key_id bigserial NOT NULL,
	key bytea PRIMARY KEY
)`

	kvLbrKidTblDef string = `CREATE TABLE IF NOT EXISTS kv_lbr_kid(
	rev_main bigint NOT NULL,
	rev_sub bigint NOT NULL,
	rev_create bigint,
	lease bigint,
	version bigint,
	key_id bigint NOT NULL,
	value bytea,
	PRIMARY KEY (rev_main, rev_sub)
)`

	kvKeyIdIdxDef string = `CREATE UNIQUE INDEX IF NOT EXISTS kv_keys_key_id_idx ON kv_keys(
	key_id
)`
)

// by-rev-shared, key id

var (
	kvLbrKidFilterKeysAndRev bob.BaseQuery[*dialect.SelectQuery] = psql.Select(
		sm.Distinct("key"),
		sm.Columns("key", "rev_main", "rev_sub", "rev_create", "lease", "version", "value"),
		sm.From(
			psql.Select(
				sm.Columns("key", "rev_main", "rev_sub", "rev_create", "lease", "version", "value"),
				sm.From("kv_lbr_kid"),
				sm.InnerJoin("kv_keys").Using("key_id"),
				sm.Where(
					psql.And(
						wKeyRangeFilter,
						psql.Raw("latest_rev").IsNull().Or(psql.Raw("rev_main").LTE(psql.Raw("latest_rev"))),
					),
				),
			),
		),
		sm.OrderBy("key").Asc(),
		sm.OrderBy("rev_main").Desc(),
		sm.OrderBy("rev_sub").Desc(),
	)

	KV_LBR_KID_RANGE_ENTRY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_range_entry",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "latest_rev bigint", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("rev_create", "rev_main AS rev_mod", "lease", "version", "key", "value"),
			sm.From(kvLbrKidFilterKeysAndRev),
			sm.Where(wRevModCreateFilter.And(psql.Raw("value").IsNotNull())),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_KID_RANGE_KEY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_range_key",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "latest_rev bigint", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"key bytea"},
		queryDef: psql.Select(
			sm.Columns("key"),
			sm.From(kvLbrKidFilterKeysAndRev),
			sm.Where(wRevModCreateFilter.And(psql.Raw("value").IsNotNull())),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_KID_RANGE_COUNT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_range_count",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "latest_rev bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"count bigint"},
		queryDef: psql.Select(
			sm.Columns("count(*)"),
			sm.From(kvLbrKidFilterKeysAndRev),
			sm.Where(wRevModCreateFilter.And(psql.Raw("value").IsNotNull())),
		),
	})

	KV_LBR_KID_ENTRY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_entry",
		FnParam:     []string{"latest_rev bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("rev_create", "rev_main AS rev_mod", "lease", "version", "key", "value"),
			sm.From("kv_lbr_kid"),
			sm.InnerJoin("kv_keys").Using("key_id"),
			sm.Where(
				psql.And(
					psql.Raw("key").IsNotNull(),
					psql.Raw("latest_rev").IsNull().Or(psql.Raw("rev_main").LTE(psql.Raw("latest_rev"))),
				),
			),
		),
	})
)

// by-rev-now-norm

var (

	// Most recent keys
	kvLbrKidNowNormTblDef string = `CREATE TABLE IF NOT EXISTS kv_lbr_kid_now_norm(
	rev_main bigint NOT NULL,
	rev_sub bigint NOT NULL,
	key bytea NOT NULL,
	PRIMARY KEY (key)
)`

	// To speed up compaction
	kvLbrKidNowNormRevIdxDef string = `CREATE UNIQUE INDEX IF NOT EXISTS kv_lbr_kid_now_norm_rev_idx ON kv_lbr_kid_now_norm(
	rev_main, rev_sub
)`

	sLbrKidNowNormEntries bob.BaseQuery[*dialect.SelectQuery] = psql.Select(
		sm.Columns("rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
		sm.From("kv_lbr_kid_now_norm").As("now"),
		sm.InnerJoin("kv_lbr_kid").Using("rev_main", "rev_sub"),
	)

	KV_LBR_KID_NOW_NORM_RANGE_ENTRY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_now_norm_range_entry",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("rev_create", "rev_main AS rev_mod", "lease", "version", "key", "value"),
			sm.From(sLbrKidNowNormEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
			sm.OrderBy("key").Asc(),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_KID_NOW_NORM_RANGE_KEY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_now_norm_range_key",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"key bytea"},
		queryDef: psql.Select(
			sm.Columns("key"),
			sm.From(sLbrKidNowNormEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
			sm.OrderBy("key").Asc(),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_KID_NOW_NORM_RANGE_COUNT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_now_norm_range_count",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"count bigint"},
		queryDef: psql.Select(
			sm.Columns("count(*)"),
			sm.From(sLbrKidNowNormEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
		),
	})

	KV_LBR_KID_NOW_NORM_DB_SIZE pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_now_norm_db_size",
		FnTableCols: []string{"size bigint"},
		queryDef: psql.Select(
			sm.Columns(
				psql.Raw("pg_total_relation_size('buckets'::regclass)").Plus(
					psql.Raw("pg_total_relation_size('bucket_data'::regclass)"),
				).Plus(
					psql.Raw("pg_total_relation_size('kv_lbr_kid'::regclass)"),
				).Plus(
					psql.Raw("pg_total_relation_size('kv_lbr_kid_now_norm'::regclass)"),
				).Plus(
					psql.Raw("pg_total_relation_size('kv_keys'::regclass)"),
				),
			),
		),
	})

	KV_LBR_KID_NOW_NORM_UPSERT pgFn[*dialect.InsertQuery] = newPgFn(pgFunctionDef[*dialect.InsertQuery]{
		FnName:  "fn_kv_lbr_kid_now_norm_upsert",
		FnParam: []string{"rev_mains bigint[]", "rev_subs bigint[]", "rev_creates bigint[]", "leases bigint[]", "versions bigint[]", "keys bytea[]", "_values bytea[]"},
		queryDef: psql.Insert(
			im.With("distinct_keys", "key").As(psql.Select(
				sm.Distinct("key"),
				sm.Columns("key"),
				sm.From(psql.Raw("unnest(keys)")).As("u", "key"),
			)),
			im.With("new_keys", "key", "key_id").As(psql.Insert(
				im.Into("kv_keys", "key"),
				im.Query(psql.Select(
					sm.Columns("key"),
					sm.From("distinct_keys"),
					sm.Where(psql.Raw("NOT EXISTS (SELECT 1 FROM kv_keys WHERE kv_keys.key = distinct_keys.key)")),
				)),
				im.OnConflict("key").DoNothing(),
				im.Returning(
					psql.Raw("key"),
					psql.Raw("key_id"),
				),
			)),
			im.With("log_upsert", "key_id", "rev_main", "rev_sub").As(
				psql.Insert(
					im.Into("kv_lbr_kid", "rev_main", "rev_sub", "rev_create", "lease", "version", "key_id", "value"),
					im.Query(
						psql.Select(
							sm.Columns("rev_main", "rev_sub", "rev_create", "lease", "version", "coalesce(new_keys.key_id, kv_keys.key_id) AS key_id", "value"),
							sm.From(psql.Raw("unnest(rev_mains, rev_subs, rev_creates, leases, versions, keys, _values)")).As("u", "rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
							sm.LeftJoin("new_keys").Using("key"),
							sm.LeftJoin("kv_keys").Using("key"),
						),
					),
					im.OnConflict("rev_main", "rev_sub").DoUpdate(
						im.SetExcluded("rev_create"),
						im.SetExcluded("version"),
						im.SetExcluded("lease"),
						im.SetExcluded("key_id"),
						im.SetExcluded("value"),
					),
					im.Returning("key_id", "rev_main", "rev_sub"),
				),
			),
			im.Into("kv_lbr_kid_now_norm", "rev_main", "rev_sub", "key"),
			im.Query(
				psql.Select(
					sm.Columns("rev_main", "rev_sub", "coalesce(new_keys.key, kv_keys.key) AS key"),
					sm.From(
						psql.Select(
							sm.Distinct("key_id"),
							sm.Columns("key_id", "rev_main", "rev_sub"),
							sm.From("log_upsert"),
							sm.Where(psql.Raw("key_id").IsNotNull()),
							sm.OrderBy("key_id").Asc(),
							sm.OrderBy("rev_main").Desc(),
							sm.OrderBy("rev_sub").Desc(),
						),
					),
					sm.LeftJoin("new_keys").Using("key_id"),
					sm.LeftJoin("kv_keys").Using("key_id"),
				),
			),
			im.OnConflict("key").DoUpdate(
				im.SetExcluded("rev_main"),
				im.SetExcluded("rev_sub"),
			),
		),
	})

	KV_LBR_KID_NOW_NORM_COMPACT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_now_norm_compact",
		FnParam:     []string{"compact_rev bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.With("delete_batch").As(psql.Delete(
				dm.FromAs("kv_lbr_kid", "l"),
				dm.Where(
					psql.And(
						psql.Raw("compact_rev").IsNull().Or(psql.Raw("l.rev_main").LTE(psql.Raw("compact_rev"))),
						psql.RawQuery("NOT EXISTS (SELECT 1 FROM kv_lbr_kid_now_norm AS now WHERE l.rev_main = now.rev_main AND l.rev_sub = now.rev_sub)"),
					),
				),
				dm.Returning(
					psql.Raw("l.rev_main"),
					psql.Raw("l.rev_sub"),
					psql.Raw("l.rev_create"),
					psql.Raw("l.lease"),
					psql.Raw("l.version"),
					psql.Raw("l.key_id"),
					psql.Raw("l.value"),
				),
			)),
			sm.Columns("rev_create", "rev_main", "lease", "version", "key", "value"),
			sm.From("delete_batch").As("d", "rev_main", "rev_sub", "rev_create", "lease", "version", "key_id", "value"),
			sm.InnerJoin("kv_keys").Using("key_id"),
			sm.OrderBy("rev_main").Asc(),
			sm.OrderBy("rev_sub").Asc(),
		),
	})
)

func kvLbrKidNowNormSchemaBundle(useOriole bool) pgSchemaBundle {
	return pgSchemaBundle{
		tableDefs: []pgTable{
			t(kvKeysTblDef, "kv_keys", useOriole, -1, -1, -1, -1),
			t(kvLbrKidTblDef, "kv_lbr_kid", useOriole, -1, -1, -1, -1),
			t(kvLbrKidNowNormTblDef, "kv_lbr_now_norm", useOriole, -1, -1, -1, -1),
		},
		indexDefs: []pgIndex{
			i(kvKeyIdIdxDef, "kv_keys_key_id_idx", useOriole, -1, -1),
			i(kvLbrKidNowNormRevIdxDef, "kv_lbr_kid_now_norm_rev_idx", useOriole, -1, -1),
		},
		selectFns: []pgFn[*dialect.SelectQuery]{
			KV_LBR_KID_NOW_NORM_RANGE_ENTRY,
			KV_LBR_KID_NOW_NORM_RANGE_KEY,
			KV_LBR_KID_NOW_NORM_RANGE_COUNT,
			KV_LBR_KID_RANGE_ENTRY,
			KV_LBR_KID_RANGE_KEY,
			KV_LBR_KID_RANGE_COUNT,
			KV_LBR_KID_NOW_NORM_DB_SIZE,
		},
		insertFns: []pgFn[*dialect.InsertQuery]{
			KV_LBR_KID_NOW_NORM_UPSERT,
		},
		deleteFns: []pgFn[*dialect.DeleteQuery]{},
		volSelectFns: []pgFn[*dialect.SelectQuery]{
			KV_LBR_KID_NOW_NORM_COMPACT,
		},
	}
}

// by-rev-now-nifs, kid
var (

	// Upcoming log entries, used for shared queries. Normalization is for sissies
	kvLbrKidNowNifsTblDef string = `CREATE TABLE IF NOT EXISTS kv_lbr_kid_now_nifs(
	rev_main bigint NOT NULL,
	rev_sub bigint NOT NULL,
	rev_create bigint,
	lease bigint,
	version bigint,
	key bytea NOT NULL,
	value bytea,
	PRIMARY KEY (key)
)`

	// To speed up compaction
	kvLbrKidNowNifsRevIdxDef string = `CREATE UNIQUE INDEX IF NOT EXISTS kv_lbr_kid_now_nifs_rev_idx ON kv_lbr_kid_now_nifs(
	rev_main, rev_sub
)`

	sLbrKidNowNifsEntries bob.BaseQuery[*dialect.SelectQuery] = psql.Select(
		sm.Columns("rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
		sm.From("kv_lbr_kid_now_nifs"),
	)

	KV_LBR_KID_NOW_NIFS_RANGE_ENTRY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_now_nifs_range_entry",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("rev_create", "rev_main AS rev_mod", "lease", "version", "key", "value"),
			sm.From(sLbrKidNowNifsEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
			sm.OrderBy("key").Asc(),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_KID_NOW_NIFS_RANGE_KEY pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_now_nifs_range_key",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "lmt bigint", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"key bytea"},
		queryDef: psql.Select(
			sm.Columns("key"),
			sm.From(sLbrKidNowNifsEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
			sm.OrderBy("key").Asc(),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	KV_LBR_KID_NOW_NIFS_RANGE_COUNT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_now_nifs_range_count",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "min_create bigint", "max_create bigint", "min_mod bigint", "max_mod bigint"},
		FnTableCols: []string{"count bigint"},
		queryDef: psql.Select(
			sm.Columns("count(*)"),
			sm.From(sLbrKidNowNifsEntries),
			sm.Where(wKeyRangeFilter.And(wRevModCreateFilter).And(psql.Raw("value").IsNotNull())),
		),
	})

	KV_LBR_KID_NOW_NIFS_DB_SIZE pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_now_nifs_db_size",
		FnTableCols: []string{"size bigint"},
		queryDef: psql.Select(
			sm.Columns(
				psql.Raw("pg_total_relation_size('buckets'::regclass)").Plus(
					psql.Raw("pg_total_relation_size('bucket_data'::regclass)"),
				).Plus(
					psql.Raw("pg_total_relation_size('kv_lbr_kid'::regclass)"),
				).Plus(
					psql.Raw("pg_total_relation_size('kv_lbr_kid_now_nifs'::regclass)"),
				).Plus(
					psql.Raw("pg_total_relation_size('kv_keys'::regclass)"),
				),
			),
		),
	})

	KV_LBR_KID_NOW_NIFS_UPSERT pgFn[*dialect.InsertQuery] = newPgFn(pgFunctionDef[*dialect.InsertQuery]{
		FnName:  "fn_kv_lbr_kid_now_nifs_upsert",
		FnParam: []string{"rev_mains bigint[]", "rev_subs bigint[]", "rev_creates bigint[]", "leases bigint[]", "versions bigint[]", "keys bytea[]", "_values bytea[]"},
		queryDef: psql.Insert(
			im.With("distinct_keys", "key").As(psql.Select(
				sm.Distinct("key"),
				sm.Columns("key"),
				sm.From(psql.Raw("unnest(keys)")).As("u", "key"),
			)),
			im.With("new_keys", "key", "key_id").As(psql.Insert(
				im.Into("kv_keys", "key"),
				im.Query(psql.Select(
					sm.Columns("key"),
					sm.From("distinct_keys"),
					sm.Where(psql.Raw("NOT EXISTS (SELECT 1 FROM kv_keys WHERE kv_keys.key = distinct_keys.key)")),
				)),
				im.OnConflict("key").DoNothing(),
				im.Returning(
					psql.Raw("key"),
					psql.Raw("key_id"),
				),
			)),
			im.With("log_upsert", "rev_main", "rev_sub", "rev_create", "lease", "version", "key_id", "value").As(
				psql.Insert(
					im.Into("kv_lbr_kid", "rev_main", "rev_sub", "rev_create", "lease", "version", "key_id", "value"),
					im.Query(
						psql.Select(
							sm.Columns("rev_main", "rev_sub", "rev_create", "lease", "version", "coalesce(new_keys.key_id, kv_keys.key_id) AS key_id", "value"),
							sm.From(psql.Raw("unnest(rev_mains, rev_subs, rev_creates, leases, versions, keys, _values)")).As("u", "rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
							sm.LeftJoin("new_keys").Using("key"),
							sm.LeftJoin("kv_keys").Using("key"),
						),
					),
					im.OnConflict("rev_main", "rev_sub").DoUpdate(
						im.SetExcluded("rev_create"),
						im.SetExcluded("version"),
						im.SetExcluded("lease"),
						im.SetExcluded("key_id"),
						im.SetExcluded("value"),
					),
					im.Returning("rev_main", "rev_sub", "rev_create", "lease", "version", "key_id", "value"),
				),
			),
			im.Into("kv_lbr_kid_now_nifs", "rev_main", "rev_sub", "rev_create", "lease", "version", "key", "value"),
			im.Query(
				psql.Select(
					sm.Columns("rev_main", "rev_sub", "rev_create", "lease", "version", "coalesce(new_keys.key, kv_keys.key) AS key", "value"),
					sm.From(
						psql.Select(
							sm.Distinct("key_id"),
							sm.Columns("key_id", "rev_main", "rev_sub", "rev_create", "lease", "version", "value"),
							sm.From("log_upsert"),
							sm.Where(psql.Raw("key_id").IsNotNull()),
							sm.OrderBy("key_id").Asc(),
							sm.OrderBy("rev_main").Desc(),
							sm.OrderBy("rev_sub").Desc(),
						),
					),
					sm.LeftJoin("new_keys").Using("key_id"),
					sm.LeftJoin("kv_keys").Using("key_id"),
				),
			),
			im.OnConflict("key").DoUpdate(
				im.SetExcluded("rev_main"),
				im.SetExcluded("rev_sub"),
				im.SetExcluded("rev_create"),
				im.SetExcluded("version"),
				im.SetExcluded("lease"),
				im.SetExcluded("value"),
			),
		),
	})

	KV_LBR_KID_NOW_NIFS_COMPACT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_kv_lbr_kid_now_nifs_compact",
		FnParam:     []string{"compact_rev bigint"},
		FnTableCols: []string{"rev_create bigint", "rev_mod bigint", "lease bigint", "version bigint", "key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.With("delete_batch").As(psql.Delete(
				dm.FromAs("kv_lbr_kid", "l"),
				dm.Where(
					psql.And(
						psql.Raw("compact_rev").IsNull().Or(psql.Raw("l.rev_main").LTE(psql.Raw("compact_rev"))),
						psql.RawQuery("NOT EXISTS (SELECT 1 FROM kv_lbr_kid_now_nifs as now WHERE l.rev_main = now.rev_main AND l.rev_sub = now.rev_sub)"),
					),
				),
				dm.Returning(
					psql.Raw("l.rev_main"),
					psql.Raw("l.rev_sub"),
					psql.Raw("l.rev_create"),
					psql.Raw("l.lease"),
					psql.Raw("l.version"),
					psql.Raw("l.key_id"),
					psql.Raw("l.value"),
				),
			)),
			sm.Columns("rev_create", "rev_main", "lease", "version", "key", "value"),
			sm.From("delete_batch").As("d", "rev_main", "rev_sub", "rev_create", "lease", "version", "key_id", "value"),
			sm.InnerJoin("kv_keys").Using("key_id"),
			sm.OrderBy("rev_main").Asc(),
			sm.OrderBy("rev_sub").Asc(),
		),
	})
)

func kvLbrKidNowNifsSchemaBundle(useOriole bool) pgSchemaBundle {
	return pgSchemaBundle{
		tableDefs: []pgTable{
			t(kvKeysTblDef, "kv_keys", useOriole, -1, -1, -1, -1),
			t(kvLbrKidTblDef, "kv_lbr_kid", useOriole, -1, -1, -1, -1),
			t(kvLbrKidNowNifsTblDef, "kv_lbr_kid_now_nifs", useOriole, -1, -1, -1, -1),
		},
		indexDefs: []pgIndex{
			i(kvKeyIdIdxDef, "kv_keys_key_id_idx", useOriole, -1, -1),
			i(kvLbrKidNowNifsRevIdxDef, "kv_lbr_kid_now_nifs_rev_idx", useOriole, -1, -1),
		},
		selectFns: []pgFn[*dialect.SelectQuery]{
			KV_LBR_KID_NOW_NIFS_RANGE_ENTRY,
			KV_LBR_KID_NOW_NIFS_RANGE_KEY,
			KV_LBR_KID_NOW_NIFS_RANGE_COUNT,
			KV_LBR_KID_RANGE_ENTRY,
			KV_LBR_KID_RANGE_KEY,
			KV_LBR_KID_RANGE_COUNT,
			KV_LBR_KID_NOW_NIFS_DB_SIZE,
		},
		insertFns: []pgFn[*dialect.InsertQuery]{
			KV_LBR_KID_NOW_NIFS_UPSERT,
		},
		deleteFns: []pgFn[*dialect.DeleteQuery]{},
		volSelectFns: []pgFn[*dialect.SelectQuery]{
			KV_LBR_KID_NOW_NIFS_COMPACT,
		},
	}
}
