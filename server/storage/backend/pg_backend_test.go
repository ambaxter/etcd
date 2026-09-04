package backend_test

import (
	"fmt"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/storage/backend"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
)

func TestPgBackendClose(t *testing.T) {
	if !backend.TEST_POSTGRES {
		t.Skip("Skipping Postgres backend test")
	}
	for _, kv := range backend.PgBackendTypes {
		testName := fmt.Sprintf("%s-%s", "TestPgBackendClose", kv)
		t.Run(testName, func(t *testing.T) {
			b, _ := betesting.NewTmpPgBackend(t, time.Hour, 10000, kv)

			// check close could work
			done := make(chan struct{}, 1)
			go func() {
				err := b.Close()
				if err != nil {
					t.Errorf("close error = %v, want nil", err)
				}
				done <- struct{}{}
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Errorf("failed to close database in 10s")
			}
		})
	}
}

/*
func TestBackendSnapshot(t *testing.T) {
}
func TestBackendBatchIntervalCommit(t *testing.T) {
}
func TestBackendDefrag(t *testing.T) {
}
func TestBackendWriteback(t *testing.T) {
}
func TestConcurrentReadTx(t *testing.T) {
}
func TestBackendWritebackForEach(t *testing.T) {
}
*/
