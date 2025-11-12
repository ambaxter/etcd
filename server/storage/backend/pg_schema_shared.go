package backend

import "github.com/stephenafamo/bob/dialect/psql"

var (
	wKeyRangeFilter psql.Expression = psql.Or(
		psql.And(
			psql.Raw("end_key").IsNull(),
			psql.Raw("key").EQ(psql.Raw("begin_key")),
		),
		psql.And(
			psql.Raw("end_key").IsNotNull(),
			psql.Raw("length(end_key)").EQ(psql.Raw("0")),
			psql.Raw("key").GTE(psql.Raw("begin_key")),
		),
		psql.And(
			psql.Raw("end_key").IsNotNull(),
			psql.Raw("key").GTE(psql.Raw("begin_key")),
			psql.Raw("key").LT(psql.Raw("end_key")),
		),
	)

	wRevModCreateFilter psql.Expression = psql.And(
		psql.Raw("min_create").IsNull().Or(psql.Raw("rev_create").GTE(psql.Raw("min_create"))),
		psql.Raw("max_create").IsNull().Or(psql.Raw("rev_create").LT(psql.Raw("max_create"))),
		psql.Raw("min_mod").IsNull().Or(psql.Raw("rev_main").GTE(psql.Raw("min_mod"))),
		psql.Raw("max_mod").IsNull().Or(psql.Raw("rev_main").LT(psql.Raw("max_mod"))),
	)
)
