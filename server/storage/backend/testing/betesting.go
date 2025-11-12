// Copyright 2021 The etcd Authors
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

package betesting

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/storage/backend"
)

var (
	postgresNameRegex, _ = regexp.Compile(`(?:^[^a-zA-Z_])|[\W]`)
	postgresNameMaxLen   = 31
	pgTestDb             = "unit_tests"
	pgTestReader         = "test_reader"
	pgTestWriter         = "test_writer"
)

func NewTmpPgBackendFromCfg(t testing.TB, bcfg backend.PgBackendConfig) (backend.Backend, string) {
	tail := time.Now().Format("06002150405")
	tail = fmt.Sprintf("_%s_%s", bcfg.PgKvType().ShortString(), tail)
	var pgTestSchema = postgresNameRegex.ReplaceAllString(t.Name(), "_")
	if len(pgTestSchema) > postgresNameMaxLen-len(tail) {
		pgTestSchema = pgTestSchema[:postgresNameMaxLen-len(tail)]
	}
	bcfg.EtcdReader = pgTestReader
	bcfg.EtcdWriter = pgTestWriter
	bcfg.EtcdDatabase = pgTestDb
	bcfg.EtcdSchema = pgTestSchema + tail

	t.Cleanup(func() {
		pool, err := pgxpool.New(t.Context(), bcfg.AdminEtcdConnectionString())
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		var cleanupStatements = []string{
			fmt.Sprintf("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s FROM %s", bcfg.EtcdSchema, bcfg.EtcdReader),
			fmt.Sprintf("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s FROM %s", bcfg.EtcdSchema, bcfg.EtcdWriter),
		}
		if t.Failed() {
			t.Logf("Test failed. Saving schema %s.%s", bcfg.EtcdDatabase, bcfg.EtcdSchema)
		} else {
			cleanupStatements = append(cleanupStatements, fmt.Sprintf("DROP SCHEMA %s CASCADE", bcfg.EtcdSchema))
		}
		for _, statement := range cleanupStatements {
			_, err = pool.Exec(context.Background(), statement)
			if err != nil {
				t.Fatal(statement, err)
			}
		}
	})

	connectionStrings := strings.Join([]string{bcfg.ReaderConnectionString(), bcfg.WriterConnectionString()}, "----")
	fmt.Println(connectionStrings)
	backend := backend.NewPg(bcfg)
	return backend, connectionStrings
}

func NewTmpBackendFromCfg(t testing.TB, bcfg backend.BackendConfig) (backend.Backend, string) {
	bcfg.Logger = zaptest.NewLogger(t)
	if backend.TEST_POSTGRES {
		pcfg := backend.PgBackendConfig{
			BackendConfig: bcfg,
		}
		return NewTmpPgBackendFromCfg(t, pcfg)
	} else {
		dir, err := os.MkdirTemp(t.TempDir(), "etcd_backend_test")
		if err != nil {
			panic(err)
		}
		tmpPath := filepath.Join(dir, "database")
		bcfg.Path = tmpPath
		return backend.New(bcfg), tmpPath
	}
}

// NewTmpBackend creates a backend implementation for testing.
func NewTmpBackend(t testing.TB, batchInterval time.Duration, batchLimit int) (backend.Backend, string) {
	if backend.TEST_POSTGRES {
		bcfg := backend.DefaultPgBackendConfig(zaptest.NewLogger(t))
		bcfg.BatchInterval, bcfg.BatchLimit = batchInterval, batchLimit
		return NewTmpPgBackendFromCfg(t, bcfg)
	} else {
		bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
		bcfg.BatchInterval, bcfg.BatchLimit = batchInterval, batchLimit
		return NewTmpBackendFromCfg(t, bcfg)
	}
}

func NewTmpPgBackend(t testing.TB, batchInterval time.Duration, batchLimit int, kvType string) (backend.Backend, string) {
	cfg := backend.DefaultPgBackendConfig(zaptest.NewLogger(t))
	cfg.PostgresKvType = kvType
	cfg.BatchInterval, cfg.BatchLimit = batchInterval, batchLimit
	return NewTmpPgBackendFromCfg(t, cfg)

}

func NewDefaultTmpPgBackend(t testing.TB, kvType string) (backend.Backend, string) {
	cfg := backend.DefaultPgBackendConfig(zaptest.NewLogger(t))
	cfg.PostgresKvType = kvType
	return NewTmpPgBackendFromCfg(t, cfg)
}

func NewDefaultTmpBackend(t testing.TB) (backend.Backend, string) {
	if backend.TEST_POSTGRES {
		return NewTmpPgBackendFromCfg(t, backend.DefaultPgBackendConfig(zaptest.NewLogger(t)))
	} else {
		return NewTmpBackendFromCfg(t, backend.DefaultBackendConfig(zaptest.NewLogger(t)))
	}
}

func Close(t testing.TB, b backend.Backend) {
	assert.NoError(t, b.Close())
}
