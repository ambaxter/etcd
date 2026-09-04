package backend

import (
	"bytes"
	"context"
	"fmt"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/dialect"
	"github.com/stephenafamo/bob/dialect/psql/sm"
	"github.com/stephenafamo/bob/expr"
	"go.uber.org/zap"
)

type pgTable struct {
	Name      string
	CreateSql string
	Options   []string
	UseOriole bool
}

func t(createSql, name string, useOriole bool, fillFactor, compress, primaryCompress, toastCompress int) pgTable {
	var options []string
	if fillFactor > 0 {
		options = append(options, fmt.Sprintf("fillfactor = %d", fillFactor))
	}
	if useOriole {
		if compress > 0 {
			options = append(options, fmt.Sprintf("compress = %d", compress))
		}
		if primaryCompress > 0 {
			options = append(options, fmt.Sprintf("primary_compress = %d", primaryCompress))
		}
		if toastCompress > 0 {
			options = append(options, fmt.Sprintf("toast_compress = %d", primaryCompress))
		}
	}
	return pgTable{
		Name:      name,
		CreateSql: createSql,
		Options:   options,
		UseOriole: useOriole,
	}
}

type pgIndex struct {
	Name      string
	CreateSql string
	Options   []string
}

func i(createSql, name string, useOriole bool, fillFactor, compress int) pgIndex {
	var options []string
	if fillFactor > 0 {
		options = append(options, fmt.Sprintf("fillfactor = %d", fillFactor))
	}
	if useOriole {
		if compress > 0 {
			options = append(options, fmt.Sprintf("compress = %d", compress))
		}
	}
	return pgIndex{
		Name:      name,
		CreateSql: createSql,
		Options:   options,
	}
}

type pgFunctionDef[E bob.Expression] struct {
	FnName      string
	FnParam     []string
	FnTableCols []string
	queryDef    bob.BaseQuery[E]
}

func (p pgFunctionDef[E]) QueryMustBuild() string {
	q, _ := p.queryDef.MustBuild(context.Background())
	return q
}

func (p pgFunctionDef[E]) CallDef() bob.BaseQuery[*dialect.SelectQuery] {
	if len(p.FnParam) > 0 {
		return psql.Select(sm.From(psql.F(p.FnName, psql.Placeholder(uint(len(p.FnParam))))))
	}
	return psql.Select(sm.From(psql.F(p.FnName)))
}

type pgFn[E bob.Expression] struct {
	f         pgFunctionDef[E]
	queryFn   string
	explainFn string
}

func newPgFn[E bob.Expression](f pgFunctionDef[E]) pgFn[E] {
	queryFn, _ := f.CallDef().MustBuild(context.Background())
	explainFn := "EXPLAIN " + queryFn
	return pgFn[E]{
		f:         f,
		queryFn:   queryFn,
		explainFn: explainFn,
	}
}

func (p pgFn[E]) Fn() string {
	return p.queryFn
}

func (p pgFn[E]) E() string {
	return p.explainFn
}

type pgSchemaBundle struct {
	tableDefs    []pgTable
	indexDefs    []pgIndex
	selectFns    []pgFn[*dialect.SelectQuery]
	immutableFns []pgFn[*dialect.SelectQuery]
	insertFns    []pgFn[*dialect.InsertQuery]
	deleteFns    []pgFn[*dialect.DeleteQuery]
	volSelectFns []pgFn[*dialect.SelectQuery]
}

var (
	tableTemplate string = `{{.CreateSql}}{{if .UseOriole}} USING ORIOLEDB{{end}}{{if .Options}} WITH ({{range $i, $opt := .Options}}{{if $i}}, {{$opt}}{{else}}{{$opt}}{{end}}{{end}}){{end}};`

	indexTemplate string = `{{.CreateSql}}{{if .Options}} WITH ({{range $i, $opt := .Options}}{{if $i}}, {{$opt}}{{else}}{{$opt}}{{end}}{{end}}){{end}};`

	immutableFnTemplate string = `CREATE OR REPLACE FUNCTION {{.FnName}}({{range $i, $col := .FnParam}}{{if $i}}, {{$col}}{{else}}{{$col}}{{end}}{{end}})
RETURNS ({{range $i, $col := .FnTableCols}}{{if $i}}, {{$col}}{{else}}{{$col}}{{end}}{{end}})
LANGUAGE sql 
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
RETURN {{.QueryMustBuild}};`

	stableFnTemplate string = `CREATE OR REPLACE FUNCTION {{.FnName}}({{range $i, $col := .FnParam}}{{if $i}}, {{$col}}{{else}}{{$col}}{{end}}{{end}})
RETURNS TABLE({{range $i, $col := .FnTableCols}}{{if $i}}, {{$col}}{{else}}{{$col}}{{end}}{{end}})
LANGUAGE sql 
STABLE
CALLED ON NULL INPUT
PARALLEL SAFE
BEGIN ATOMIC
{{.QueryMustBuild}};
END`

	volatileFnTemplate string = `CREATE OR REPLACE FUNCTION {{.FnName}}({{range $i, $col := .FnParam}}{{if $i}}, {{$col}}{{else}}{{$col}}{{end}}{{end}})
{{if .FnTableCols}}RETURNS TABLE({{range $i, $col := .FnTableCols}}{{if $i}}, {{$col}}{{else}}{{$col}}{{end}}{{end}}){{else}}RETURNS void{{end}}
LANGUAGE sql 
VOLATILE
CALLED ON NULL INPUT
PARALLEL UNSAFE
BEGIN ATOMIC
{{.QueryMustBuild}};
END`
)

type pgSetup struct {
	bcfg                *PgBackendConfig
	tableNameSet        map[string]struct{}
	indexNameSet        map[string]struct{}
	fnNameSet           map[string]struct{}
	fnSqlSet            map[string]struct{}
	tableTemplate       *template.Template
	indexTemplate       *template.Template
	immutableFnTemplate *template.Template
	stableFnTemplate    *template.Template
	volatileFnTemplate  *template.Template
}

func newPgSetup(bcfg *PgBackendConfig) pgSetup {
	tableTemplate := template.Must(template.New("table").Parse(tableTemplate))
	indexTemplate := template.Must(template.New("index").Parse(indexTemplate))
	immutableFnTemplate := template.Must(template.New("immutable").Parse(immutableFnTemplate))
	stableFnTemplate := template.Must(template.New("stable").Parse(stableFnTemplate))
	volatileFnTemplate := template.Must(template.New("volatile").Parse(volatileFnTemplate))
	return pgSetup{
		bcfg:                bcfg,
		tableNameSet:        make(map[string]struct{}),
		indexNameSet:        make(map[string]struct{}),
		fnNameSet:           make(map[string]struct{}),
		fnSqlSet:            make(map[string]struct{}),
		tableTemplate:       tableTemplate,
		indexTemplate:       indexTemplate,
		immutableFnTemplate: immutableFnTemplate,
		stableFnTemplate:    stableFnTemplate,
		volatileFnTemplate:  volatileFnTemplate,
	}

}

// silly retry function
func (p pgSetup) requireConnection(connectionString string) *pgxpool.Pool {
	maxRetries := 20
	backoff := 2
	initialWaitTime := 50 * time.Millisecond
	maxWaitTime := 10 * time.Second
	waitTime := initialWaitTime
	connectionWarningTimeout := 10 * time.Second
	connectStopC := make(chan struct{})
	defer close(connectStopC)
	start := time.Now()
	go func() {
		ticker := time.NewTicker(connectionWarningTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.bcfg.Logger.Warn(
					"pg connection is taking too long",
					zap.Duration("taking", time.Since(start)),
				)

			case <-connectStopC:
				return
			}
		}
	}()
	var pool *pgxpool.Pool
	var err error
	for retyCount := 0; retyCount < maxRetries; retyCount += 1 {
		time.Sleep(waitTime)
		pool, err = pgxpool.New(context.Background(), connectionString)
		if err == nil {
			err = pool.Ping(context.Background())
		}
		if err != nil {
			time.Sleep(waitTime)
			waitTime = time.Duration(waitTime.Milliseconds() * int64(backoff))
			waitTime = min(waitTime, maxWaitTime)
		} else {
			return pool
		}
	}
	p.bcfg.Logger.Fatal(
		"pg connection failed", zap.String("connectionString", connectionString),
		zap.Duration("taking", time.Since(start)))
	return nil
}

func (p pgSetup) checkExists(pool *pgxpool.Pool, query string, arg string) bool {
	var i int
	row := pool.QueryRow(context.Background(), query, arg)
	err := row.Scan(&i)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false
		} else {
			p.bcfg.Logger.Fatal("Check Configuration Error", zap.String("query", query), zap.String("arg", arg), zap.Error(err))
		}
	}
	return true
}

func (p pgSetup) hasDb(pool *pgxpool.Pool, db string) bool {
	return p.checkExists(pool, "SELECT 1 FROM pg_database WHERE datname = $1 LIMIT 1", db)
}

func (p pgSetup) hasUser(pool *pgxpool.Pool, username string) bool {
	return p.checkExists(pool, "SELECT 1 FROM pg_roles WHERE rolname=$1 LIMIT 1;", username)
}

func (p pgSetup) hasExt(pool *pgxpool.Pool, extension string) bool {
	return p.checkExists(pool, "SELECT 1 FROM pg_available_extensions WHERE name=$1 LIMIT 1;", extension)
}

func (p pgSetup) registerTable(pool *pgxpool.Pool, table pgTable) {
	p.bcfg.Logger.Info("Registering", zap.String("table", table.Name))
	_, ok := p.tableNameSet[table.Name]
	if ok {
		p.bcfg.Logger.Fatal("Already registered table", zap.String("name", table.Name))
	}
	buf := &bytes.Buffer{}
	err := p.tableTemplate.Execute(buf, table)
	if err != nil {
		p.bcfg.Logger.Error("Error building table", zap.Error(err))
	}
	q := buf.String()
	_, err = pool.Exec(context.Background(), q)
	if err != nil {
		p.bcfg.Logger.Fatal("Error creating table", zap.String("query", q), zap.Error(err))
	}
	p.tableNameSet[table.Name] = struct{}{}
}

func (p pgSetup) registerIndex(pool *pgxpool.Pool, index pgIndex) {
	p.bcfg.Logger.Info("Registering", zap.String("index", index.Name))
	_, ok := p.indexNameSet[index.Name]
	if ok {
		p.bcfg.Logger.Fatal("Already registered index", zap.String("name", index.Name))
	}
	buf := &bytes.Buffer{}
	err := p.indexTemplate.Execute(buf, index)
	if err != nil {
		p.bcfg.Logger.Error("Error building index", zap.Error(err))
	}
	q := buf.String()
	_, err = pool.Exec(context.Background(), q)
	if err != nil {
		print(q)
		p.bcfg.Logger.Fatal("Error creating index", zap.String("query", q), zap.Error(err))
	}
	p.indexNameSet[index.Name] = struct{}{}
}

func (p pgSetup) registerImmutableFn(pool *pgxpool.Pool, f pgFunctionDef[*dialect.SelectQuery]) {
	registerFunction(&p, pool, f, p.immutableFnTemplate)
}

func (p pgSetup) registerSelectFn(pool *pgxpool.Pool, f pgFunctionDef[*dialect.SelectQuery]) {
	registerFunction(&p, pool, f, p.stableFnTemplate)
}

func (p pgSetup) registerStableFn(pool *pgxpool.Pool, f pgFunctionDef[expr.Clause]) {
	registerFunction(&p, pool, f, p.stableFnTemplate)
}

func (p pgSetup) registerInsertFn(pool *pgxpool.Pool, f pgFunctionDef[*dialect.InsertQuery]) {
	registerFunction(&p, pool, f, p.volatileFnTemplate)
}

func (p pgSetup) registerDeleteFn(pool *pgxpool.Pool, f pgFunctionDef[*dialect.DeleteQuery]) {
	registerFunction(&p, pool, f, p.volatileFnTemplate)
}

func (p pgSetup) registerVolSelectFn(pool *pgxpool.Pool, f pgFunctionDef[*dialect.SelectQuery]) {
	registerFunction(&p, pool, f, p.volatileFnTemplate)
}

func registerFunction[E bob.Expression](p *pgSetup, pool *pgxpool.Pool, f pgFunctionDef[E], template *template.Template) {
	p.bcfg.Logger.Info("Registering", zap.String("fn", f.FnName))
	_, ok := p.fnNameSet[f.FnName]
	if ok {
		p.bcfg.Logger.Fatal("Already registered fn", zap.String("name", f.FnName))
	}
	buf := &bytes.Buffer{}
	err := template.Execute(buf, f)
	if err != nil {
		p.bcfg.Logger.Error("Error building fn", zap.Error(err))
	}
	q := buf.String()
	_, err = pool.Exec(context.Background(), q)
	if err != nil {
		print(q)
		p.bcfg.Logger.Fatal("Error registering fn", zap.String("query", q), zap.Error(err))
	}
	p.fnNameSet[f.FnName] = struct{}{}
}

func (p pgSetup) configurePostgresDb() {
	adminUrl := p.bcfg.AdminConnectionString()
	adminPool := p.requireConnection(adminUrl)
	defer adminPool.Close()

	hasDb := p.hasDb(adminPool, p.bcfg.EtcdDatabase)

	if !hasDb {
		_, err := adminPool.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s", p.bcfg.EtcdDatabase))
		if err != nil {
			p.bcfg.Logger.Fatal("Error Creating Database", zap.Error(err))
		}
	}

	useOriole := p.hasExt(adminPool, "orioledb")
	hasReader := p.hasUser(adminPool, p.bcfg.EtcdReader)
	hasWriter := p.hasUser(adminPool, p.bcfg.EtcdWriter)
	p.bcfg.UseOriole = useOriole
	batch := pgx.Batch{}
	if useOriole {
		batch.Queue("CREATE EXTENSION IF NOT EXISTS orioledb")
	}
	if !hasReader {
		batch.Queue(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", p.bcfg.EtcdReader, p.bcfg.EtcdReader))
		batch.Queue(fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", p.bcfg.EtcdDatabase, p.bcfg.EtcdReader))
	}
	if !hasWriter {
		batch.Queue(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", p.bcfg.EtcdWriter, p.bcfg.EtcdWriter))
		batch.Queue(fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", p.bcfg.EtcdDatabase, p.bcfg.EtcdWriter))
	}
	results := adminPool.SendBatch(context.Background(), &batch)
	err := results.Close()
	if err != nil {
		p.bcfg.Logger.Fatal("Error Creating Database Config", zap.Error(err))
	}
}

func (p pgSetup) configureEtcdDb() {
	etcdAdminUrl := p.bcfg.AdminEtcdConnectionString()
	adminPool := p.requireConnection(etcdAdminUrl)
	defer adminPool.Close()
	adminPool.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", p.bcfg.EtcdSchema))
}

func (p pgSetup) grantSchemaPermissions() {
	etcdAdminUrl := p.bcfg.AdminEtcdConnectionString()
	adminPool := p.requireConnection(etcdAdminUrl)
	defer adminPool.Close()
	batch := pgx.Batch{}
	batch.Queue(fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", p.bcfg.EtcdSchema, p.bcfg.EtcdReader))
	batch.Queue(fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s", p.bcfg.EtcdSchema, p.bcfg.EtcdReader))
	batch.Queue(fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", p.bcfg.EtcdSchema, p.bcfg.EtcdWriter))
	batch.Queue(fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE, MAINTAIN ON ALL TABLES IN SCHEMA %s TO %s", p.bcfg.EtcdSchema, p.bcfg.EtcdWriter))
	batch.Queue(fmt.Sprintf("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s", p.bcfg.EtcdSchema, p.bcfg.EtcdWriter))
	results := adminPool.SendBatch(context.Background(), &batch)
	err := results.Close()
	if err != nil {
		p.bcfg.Logger.Fatal("Error Creating Database Config", zap.Error(err))
	}
}

func (p pgSetup) registerSchemaBundle(pool *pgxpool.Pool, bundle pgSchemaBundle) {
	for _, tbl := range bundle.tableDefs {
		p.registerTable(pool, tbl)
	}
	for _, idx := range bundle.indexDefs {
		p.registerIndex(pool, idx)
	}
	for _, fn := range bundle.selectFns {
		p.registerSelectFn(pool, fn.f)
	}
	for _, fn := range bundle.insertFns {
		p.registerInsertFn(pool, fn.f)
	}
	for _, fn := range bundle.deleteFns {
		p.registerDeleteFn(pool, fn.f)
	}
	for _, fn := range bundle.volSelectFns {
		p.registerVolSelectFn(pool, fn.f)
	}
}

func (p pgSetup) configureEtcdSchema() {
	schemaAdminUrl := p.bcfg.AdminSchemaConnectionString()
	adminPool := p.requireConnection(schemaAdminUrl)
	defer adminPool.Close()

	bucketBundle := bucketSchemaBundle(p.bcfg.UseOriole)
	p.registerSchemaBundle(adminPool, bucketBundle)

	var kvBundle pgSchemaBundle
	switch p.bcfg.PgKvType() {
	case KvBucketKeys:
		kvBundle = bucketKeysSchemaBundle(p.bcfg.UseOriole)
	case KvLbrNowNorm:
		kvBundle = kvLbrNowNormSchemaBundle(p.bcfg.UseOriole)
	case KvLbrNowNifs:
		kvBundle = kvLbrNowNifsSchemaBundle(p.bcfg.UseOriole)
	case KvLbrKidNowNorm:
		kvBundle = kvLbrKidNowNormSchemaBundle(p.bcfg.UseOriole)
	case KvLbrKidNowNifs:
		kvBundle = kvLbrKidNowNifsSchemaBundle(p.bcfg.UseOriole)
	}
	p.registerSchemaBundle(adminPool, kvBundle)
}

func RunPgSetup(bcfg *PgBackendConfig) {
	setup := newPgSetup(bcfg)
	setup.configurePostgresDb()
	setup.configureEtcdDb()
	setup.configureEtcdSchema()
	setup.grantSchemaPermissions()
}
