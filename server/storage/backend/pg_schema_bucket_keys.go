package backend

import (
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/dialect"
	"github.com/stephenafamo/bob/dialect/psql/dm"
	"github.com/stephenafamo/bob/dialect/psql/im"
	"github.com/stephenafamo/bob/dialect/psql/sm"
)

var (
	keyDataTblDef string = `CREATE TABLE IF NOT EXISTS key_data(
	key bytea PRIMARY KEY,
	value bytea
)`

	BUCKET_KEYS_DB_SIZE pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_bucket_keys_db_size",
		FnTableCols: []string{"size bigint"},
		queryDef: psql.Select(
			sm.Columns(
				psql.Raw("pg_total_relation_size('buckets'::regclass)").Plus(
					psql.Raw("pg_total_relation_size('bucket_data'::regclass)"),
				).Plus(
					psql.Raw("pg_total_relation_size('key_data'::regclass)"),
				),
			),
		),
	})

	// Key Bucket Functions
	KEYS_SCAN pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_key_scan",
		FnParam:     []string{},
		FnTableCols: []string{"key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("key", "value"),
			sm.From("key_data"),
			sm.OrderBy("key").Asc(),
		),
	})

	KEYS_RANGE pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_key_range",
		FnParam:     []string{"begin_key bytea", "end_key bytea", "lmt bigint"},
		FnTableCols: []string{"key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("key", "value"),
			sm.From("key_data"),
			sm.Where(wKeyRangeFilter),
			sm.Limit(psql.Raw("lmt")),
		),
	})
	KEYS_EXACT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_key_exact",
		FnParam:     []string{"bucket_keys bytea[]"},
		FnTableCols: []string{"key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("key", "value"),
			sm.From("key_data"),
			sm.InnerJoin(psql.Raw("unnest(bucket_keys) as e(key)")).Using("key"),
		),
	})

	KEYS_BATCH_UPSERT_KEY pgFn[*dialect.InsertQuery] = newPgFn(pgFunctionDef[*dialect.InsertQuery]{
		FnName:  "fn_key_batch_upsert_key",
		FnParam: []string{"keys bytea[]", "_values bytea[]"},
		queryDef: psql.Insert(
			im.IntoAs("key_data", "k", "key", "value"),
			im.Query(psql.Select(
				sm.Columns("u.key", "u.value"),
				sm.From(psql.Raw("unnest(keys, _values)")).As("u", "key", "value"),
			)),
			im.OnConflict("key").DoUpdate(
				im.SetExcluded("value"),
			),
		),
	})

	KEYS_BATCH_DELETE_KEY pgFn[*dialect.DeleteQuery] = newPgFn(pgFunctionDef[*dialect.DeleteQuery]{
		FnName:  "fn_key_batch_delete_keys",
		FnParam: []string{"keys bytea[]"},
		queryDef: psql.Delete(
			dm.FromAs("key_data", "k"),
			dm.Using(psql.Raw("unnest(keys)")).As("d", "key"),
			dm.Where(psql.Raw("k.key").EQ(psql.Raw("d.key"))),
		),
	})

	KEYS_BATCH_TRUNCATE pgFn[*dialect.DeleteQuery] = newPgFn(pgFunctionDef[*dialect.DeleteQuery]{
		FnName: "fn_key_batch_truncate_keys",
		queryDef: psql.Delete(
			dm.From("key_data"),
		),
	})
)

func bucketKeysSchemaBundle(useOriole bool) pgSchemaBundle {
	return pgSchemaBundle{
		tableDefs: []pgTable{
			t(keyDataTblDef, "key_data", useOriole, -1, -1, -1, -1),
		},
		selectFns: []pgFn[*dialect.SelectQuery]{
			BUCKET_KEYS_DB_SIZE,
			KEYS_SCAN,
			KEYS_RANGE,
			KEYS_EXACT,
		},
		insertFns: []pgFn[*dialect.InsertQuery]{
			KEYS_BATCH_UPSERT_KEY,
		},
		deleteFns: []pgFn[*dialect.DeleteQuery]{
			KEYS_BATCH_DELETE_KEY,
			KEYS_BATCH_TRUNCATE,
		},
	}
}
