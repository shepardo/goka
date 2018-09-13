package leveldb

import (
	"testing"

	"github.com/lovoo/goka/storage"
	"github.com/lovoo/goka/storage/storagetest"
)

func TestLevelDB(t *testing.T) {
	storagetest.Suite(t, func(t *testing.T) (storage.Storage, storagetest.TeardownFunc) {
		tmp, teardown := storagetest.TempDir(t, "", "")
		st, err := Builder(tmp)("test-topic", 0)
		if err != nil {
			t.Fatalf("error building storage: %v", err)
		}

		return st, teardown
	})
}
