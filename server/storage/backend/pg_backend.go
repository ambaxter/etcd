package backend

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var (
	SNAPSHOT_BATCH_LIMIT uint64 = 100000
	DB_SCAN_SQL          string = `
		(SELECT b.name, d.key, d.value FROM buckets as b
		JOIN bucket_data as d ON (b.id = d.bucket_id)
		ORDER BY b.name ASC, d.key ASC)
		UNION ALL 
		(SELECT 'key'::bytea, k.key, k.value FROM key_data as k
		ORDER BY k.key)
	`
)

type PgKvType int

const (
	KvBucket PgKvType = iota
	KvBucketKeys
	KvLbrNowNorm
	KvLbrNowNifs
	KvLbrKidNowNorm
	KvLbrKidNowNifs
)

func (s PgKvType) String() string {
	switch s {
	case KvBucket:
		return "bucket"
	case KvBucketKeys:
		return "bucket_keys"
	case KvLbrNowNorm:
		return "lbr_now_norm"
	case KvLbrNowNifs:
		return "lbr_now_nifs"
	case KvLbrKidNowNorm:
		return "lbr_kid_now_norm"
	case KvLbrKidNowNifs:
		return "lbr_kid_now_nifs"
	default:
		panic(fmt.Sprintf("Unknown PgKvType: %d", int(s)))
	}
}

func (s PgKvType) ShortString() string {
	switch s {
	case KvBucket:
		return "bu"
	case KvBucketKeys:
		return "bk"
	case KvLbrNowNorm:
		return "rn"
	case KvLbrNowNifs:
		return "rf"
	case KvLbrKidNowNorm:
		return "rnk"
	case KvLbrKidNowNifs:
		return "rfk"
	default:
		panic(fmt.Sprintf("Unknown PgKvType: %d", int(s)))
	}
}

var (
	PgBackendTypes []string = []string{
		KvBucket.String(),
		KvBucketKeys.String(),
		KvLbrNowNorm.String(),
		KvLbrNowNifs.String(),
		KvLbrKidNowNorm.String(),
		KvLbrKidNowNifs.String(),
	}

	PgKvBackendTypes []string = []string{
		KvLbrNowNorm.String(),
		KvLbrNowNifs.String(),
		KvLbrKidNowNorm.String(),
		KvLbrKidNowNifs.String(),
	}
)

type pgBackend struct {
	lg *zap.Logger

	pgReadConnectionString  string
	pgWriteConnectionString string

	kvReadBufferCache *PgKvBuffer[*PgKvLogEntry]

	readPool  *pgxpool.Pool
	writePool *pgxpool.Pool

	kvType    PgKvType
	useOriole bool
}

type PgBackendConfig struct {
	BackendConfig

	Host                    string
	Port                    uint32
	AdminUser               string
	AdminPassword           string
	AdminDb                 string
	EtcdReader              string
	EtcdWriter              string
	EtcdDatabase            string
	EtcdSchema              string
	Options                 []string
	PgReadConnectionString  string
	PgWriteConnectionString string
	PostgresKvType          string
	UseOriole               bool
}

func (c *PgBackendConfig) dbConnectionString(host string, port uint32, username string, password string, database string, options ...string) string {
	if c.UseOriole {
		options = append(options, "default_table_access_method=orioledb")
	}
	options = append(options, "default_toast_compression=lz4")

	if host[0] == '/' {
		options = append(options, "host="+host)
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?%s", username, password, "", port, database, strings.Join(options, "&"))
	} else {
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?%s", username, password, host, port, database, strings.Join(options, "&"))
	}
}

func (c *PgBackendConfig) AdminConnectionString() string {
	return c.dbConnectionString(c.Host, c.Port, c.AdminUser, c.AdminPassword, c.AdminDb, "sslmode=disable")
}

func (c *PgBackendConfig) AdminEtcdConnectionString() string {
	return c.dbConnectionString(c.Host, c.Port, c.AdminUser, c.AdminPassword, c.EtcdDatabase, "sslmode=disable")
}

func (c *PgBackendConfig) AdminSchemaConnectionString() string {
	return c.dbConnectionString(c.Host, c.Port, c.AdminUser, c.AdminPassword, c.EtcdDatabase, "sslmode=disable", "search_path="+c.EtcdSchema)
}

func (c *PgBackendConfig) ReaderConnectionString() string {
	if len(c.PgReadConnectionString) > 0 {
		return c.PgReadConnectionString
	} else {
		return c.dbConnectionString(c.Host, c.Port, c.EtcdReader, c.EtcdReader, c.EtcdDatabase, "sslmode=disable", "default_transaction_read_only=true", "search_path="+c.EtcdSchema)
	}

}

func (c *PgBackendConfig) WriterConnectionString() string {
	if len(c.PgWriteConnectionString) > 0 {
		return c.PgWriteConnectionString
	} else {
		return c.dbConnectionString(c.Host, c.Port, c.EtcdWriter, c.EtcdWriter, c.EtcdDatabase, "sslmode=disable", "search_path="+c.EtcdSchema)
	}
}

func (c *PgBackendConfig) PgKvType() PgKvType {
	var kvType PgKvType
	switch c.PostgresKvType {

	case KvBucket.String(), "":
		kvType = KvBucket
	case KvBucketKeys.String():
		kvType = KvBucketKeys
	case KvLbrNowNorm.String():
		kvType = KvLbrNowNorm
	case KvLbrNowNifs.String():
		kvType = KvLbrNowNifs
	case KvLbrKidNowNorm.String():
		kvType = KvLbrKidNowNorm
	case KvLbrKidNowNifs.String():
		kvType = KvLbrKidNowNifs
	default:
		c.Logger.Panic("Unknown PgKvType", zap.Strings("expected", PgBackendTypes), zap.String("received", c.PostgresKvType))
	}
	return kvType
}

func DefaultPgBackendConfig(lg *zap.Logger) PgBackendConfig {
	host := "localhost"
	if _, err := os.Stat("/var/run/postgresql"); !os.IsNotExist(err) {
		host = "/var/run/postgresql"
	}
	pgbc := PgBackendConfig{
		BackendConfig: BackendConfig{
			BatchInterval: defaultBatchInterval,
			BatchLimit:    defaultBatchLimit,
			Logger:        lg,
		},
		Host:          host,
		Port:          5432,
		AdminUser:     "postgres",
		AdminPassword: "mysecretpassword",
		AdminDb:       "postgres",
		EtcdReader:    "etcd_reader",
		EtcdWriter:    "etcd_writer",
		EtcdDatabase:  "pgetcd",
		EtcdSchema:    "etcd"}

	return pgbc
}

func NewPg(bcfg PgBackendConfig) Backend {
	return newPgBackend(bcfg)
}

type pBucket struct {
	name  []byte
	isKey bool
}

func (b pBucket) ID() BucketID            { panic("pseudo bucket") }
func (b pBucket) Name() []byte            { return b.name }
func (b pBucket) String() string          { return string(b.Name()) }
func (b pBucket) IsSafeRangeBucket() bool { panic("pseudo bucket") }

func (b pBucket) IsKeys() bool { return b.isKey }

func RecoverPg(bcfg PgBackendConfig, snapPath string) Backend {
	lg := bcfg.Logger
	bopts := &bolt.Options{}
	bopts.NoFreelistSync = true
	db, err := bolt.Open(snapPath, 0o600, bopts)
	if err != nil {
		lg.Panic("failed to open database", zap.String("path", snapPath), zap.Error(err))
	}
	defer db.Close()
	pgResetDb(bcfg)
	var currentCount *int = new(int)
	b := newPgBackend(bcfg)
	bTx := b.BatchTx()
	// I guess?
	bTx.LockInsideApply()
	db.View(func(tx *bolt.Tx) error {
		tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			pB := Bucket(pBucket{name: name, isKey: reflect.DeepEqual(name, []byte("key"))})
			bTx.UnsafeCreateBucket(pB)
			*currentCount += 1
			b.ForEach(func(k, v []byte) error {
				bTx.UnsafePut(pB, k, v)
				*currentCount += 1
				if *currentCount > bcfg.BatchLimit {
					bTx.Unlock()
					bTx.LockInsideApply()
				}
				return nil
			})
			return nil
		})
		return nil
	})
	bTx.Unlock()
	bTx.Commit()
	return b
}

func pgResetDb(bcfg PgBackendConfig) {
	lg := bcfg.Logger
	adminUrl := bcfg.AdminConnectionString()
	adminPool, err := pgxpool.New(context.Background(), adminUrl)
	if err != nil {
		lg.Fatal("failed to open database with", zap.String("connectionString", adminUrl), zap.Error(err))
	}
	defer adminPool.Close()
	var adminStatements = []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS %s FORCE", bcfg.EtcdDatabase),
	}
	for _, statement := range adminStatements {
		_, err = adminPool.Exec(context.Background(), statement)
		if err != nil {
			lg.Fatal("failed to execute admin setup", zap.String("statement", statement), zap.Error(err))
		}
	}
}

func newPgBackend(bcfg PgBackendConfig) *backend {
	lg := bcfg.Logger
	// TODO: handle existing connection info more gracefully
	if len(bcfg.PgReadConnectionString) != 0 {
		lg.Panic("We don't handle this yet with the new system, yet")
	}

	RunPgSetup(&bcfg)

	// TODO: Pool per responsibility?
	readerConnectionString := bcfg.ReaderConnectionString()
	readerConfig, err := pgxpool.ParseConfig(readerConnectionString)
	readerConfig.MaxConns = 108
	readerConfig.MinIdleConns = 8
	if err != nil {
		lg.Panic("failed to parse config for", zap.String("connectionString", readerConnectionString), zap.Error(err))
	}
	readPool, err := pgxpool.NewWithConfig(context.Background(), readerConfig)
	if err != nil {
		lg.Panic("failed to open database for", zap.String("connectionString", readerConnectionString), zap.Error(err))
	}
	writerConnectionString := bcfg.WriterConnectionString()
	writePool, err := pgxpool.New(context.Background(), writerConnectionString)
	if err != nil {
		lg.Panic("failed to open database for write", zap.String("connectionString", writerConnectionString), zap.Error(err))
	}
	kvType := bcfg.PgKvType()

	b := &backend{
		batchInterval: bcfg.BatchInterval,
		batchLimit:    bcfg.BatchLimit,

		readTx: &readTx{
			baseReadTx: baseReadTx{
				lg: lg,
				buf: txReadBuffer{
					txBuffer:   txBuffer{make(map[BucketID]*bucketBuffer)},
					bufVersion: 0,
				},
				buckets: make(map[BucketID]*bolt.Bucket),
				txWg:    new(sync.WaitGroup),
				txMu:    new(sync.RWMutex),
				pgTx: &pgReadTx{
					pgSharedTx: pgSharedTx{
						lg:     lg,
						kvType: kvType,
					},
					readPool: readPool,
				},
			},
		},
		txReadBufferCache: txReadBufferCache{
			mu:         sync.Mutex{},
			bufVersion: 0,
			buf:        nil,
		},

		stopc: make(chan struct{}),
		donec: make(chan struct{}),

		lg: lg,
		pgBackend: &pgBackend{
			pgReadConnectionString:  readerConnectionString,
			pgWriteConnectionString: writerConnectionString,
			readPool:                readPool,
			writePool:               writePool,
			kvType:                  kvType,
			useOriole:               bcfg.UseOriole,
		},
	}

	b.batchTx = newPgBatchTxBuffered(b)

	b.hooks = bcfg.Hooks

	go b.run()
	return b
}
