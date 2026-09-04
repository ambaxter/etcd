package backend

import (
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/dialect"
	"github.com/stephenafamo/bob/dialect/psql/dm"
	"github.com/stephenafamo/bob/dialect/psql/im"
	"github.com/stephenafamo/bob/dialect/psql/sm"
)

// bucket-based schema
var (
	bucketsTblDef string = `CREATE TABLE IF NOT EXISTS buckets(
	bucket_id serial PRIMARY KEY,
	name bytea UNIQUE NOT NULL
)`

	bucketsDataTblDef string = `CREATE TABLE IF NOT EXISTS bucket_data(
	bucket_id integer NOT NULL REFERENCES buckets(bucket_id) ON DELETE CASCADE,
	key bytea NOT NULL,
	value bytea,
	PRIMARY KEY(bucket_id, key)
)`

	BUCKETS_DB_SIZE pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_bucket_db_size",
		FnTableCols: []string{"size bigint"},
		queryDef: psql.Select(
			sm.Columns(
				psql.Raw("pg_total_relation_size('buckets'::regclass)").Plus(
					psql.Raw("pg_total_relation_size('bucket_data'::regclass)"),
				),
			),
		),
	})

	// Bucket functions
	BUCKETS_SCAN pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_bucket_scan",
		FnParam:     []string{"bucket_name bytea"},
		FnTableCols: []string{"key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("key", "value"),
			sm.From("bucket_data"),
			sm.InnerJoin("buckets").Using("bucket_id"),
			sm.Where(psql.Raw("name").EQ(psql.Raw("bucket_name"))),
			sm.OrderBy("key").Asc(),
		),
	})

	BUCKETS_RANGE pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_bucket_range",
		FnParam:     []string{"bucket_name bytea", "begin_key bytea", "end_key bytea", "lmt bigint"},
		FnTableCols: []string{"key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("key", "value"),
			sm.From("bucket_data"),
			sm.InnerJoin("buckets").Using("bucket_id"),
			sm.Where(
				psql.And(
					psql.Raw("name").EQ(psql.Raw("bucket_name")),
					wKeyRangeFilter,
				),
			),
			sm.OrderBy("key").Asc(),
			sm.Limit(psql.Raw("lmt")),
		),
	})

	BUCKETS_EXACT pgFn[*dialect.SelectQuery] = newPgFn(pgFunctionDef[*dialect.SelectQuery]{
		FnName:      "fn_bucket_exact",
		FnParam:     []string{"bucket_name bytea", "bucket_keys bytea[]"},
		FnTableCols: []string{"key bytea", "value bytea"},
		queryDef: psql.Select(
			sm.Columns("key", "value"),
			sm.From("bucket_data"),
			sm.InnerJoin("buckets").Using("bucket_id"),
			sm.InnerJoin(psql.Raw("unnest(bucket_keys) AS e(key)")).Using("key"),
			sm.Where(psql.Raw("name").EQ(psql.Raw("bucket_name"))),
		),
	})

	BUCKETS_CREATE_BUCKET pgFn[*dialect.InsertQuery] = newPgFn(pgFunctionDef[*dialect.InsertQuery]{
		FnName:  "fn_create_bucket",
		FnParam: []string{"bucket_name bytea"},
		queryDef: psql.Insert(
			im.Into("buckets", "name"),
			im.Values(psql.Raw("bucket_name")),
			im.OnConflict("name").DoNothing(),
		),
	})

	BUCKETS_BATCH_UPSERT_KEY pgFn[*dialect.InsertQuery] = newPgFn(pgFunctionDef[*dialect.InsertQuery]{
		FnName:  "fn_bucket_batch_upsert_key",
		FnParam: []string{"bucket_name bytea", "keys bytea[]", "_values bytea[]"},
		queryDef: psql.Insert(
			im.IntoAs("bucket_data", "bd", "bucket_id", "key", "value"),
			im.Query(psql.Select(
				sm.Columns("b.bucket_id", "u.key", "u.value"),
				sm.From(psql.Raw("buckets AS b, unnest(keys, _values) AS u(key, value)")),
				sm.Where(psql.Raw("b.name").EQ(psql.Raw("bucket_name"))),
			)),
			im.OnConflict("bucket_id", "key").DoUpdate(
				im.SetExcluded("value"),
			),
		),
	})

	BUCKETS_DELETE_BUCKET pgFn[*dialect.DeleteQuery] = newPgFn(pgFunctionDef[*dialect.DeleteQuery]{
		FnName:  "fn_delete_bucket",
		FnParam: []string{"bucket_name bytea"},
		queryDef: psql.Delete(
			dm.From("buckets"),
			dm.Where(psql.Raw("name").EQ(psql.Raw("bucket_name"))),
		),
	})

	BUCKETS_BATCH_DELETE_KEY pgFn[*dialect.DeleteQuery] = newPgFn(pgFunctionDef[*dialect.DeleteQuery]{
		FnName:  "fn_bucket_batch_delete_keys",
		FnParam: []string{"bucket_name bytea", "keys bytea[]"},
		queryDef: psql.Delete(
			dm.FromAs("bucket_data", "bd"),
			dm.Using(psql.Raw("buckets b, unnest($2::bytea[]) AS d(key)")),
			dm.Where(
				psql.And(
					psql.Raw("b.name").EQ(psql.Raw("bucket_name")),
					psql.Raw("b.bucket_id").EQ(psql.Raw("bd.bucket_id")),
					psql.Raw("bd.key").EQ(psql.Raw("d.key")),
				),
			),
		),
	})
)

func bucketSchemaBundle(useOriole bool) pgSchemaBundle {
	return pgSchemaBundle{
		tableDefs: []pgTable{
			t(bucketsTblDef, "buckets", useOriole, -1, -1, -1, -1),
			t(bucketsDataTblDef, "bucket_data", useOriole, -1, -1, -1, -1),
		},
		selectFns: []pgFn[*dialect.SelectQuery]{
			BUCKETS_DB_SIZE,
			BUCKETS_SCAN,
			BUCKETS_RANGE,
			BUCKETS_EXACT,
		},
		insertFns: []pgFn[*dialect.InsertQuery]{
			BUCKETS_CREATE_BUCKET,
			BUCKETS_BATCH_UPSERT_KEY,
		},
		deleteFns: []pgFn[*dialect.DeleteQuery]{
			BUCKETS_DELETE_BUCKET,
			BUCKETS_BATCH_DELETE_KEY,
		},
	}
}
