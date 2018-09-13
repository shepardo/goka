package simple

import (
	"testing"

	"github.com/lovoo/goka/storage"
	"github.com/lovoo/goka/storage/storagetest"
)

func TestKVSimple(t *testing.T) {
	storagetest.Suite(t, func(t *testing.T) (storage.Storage, storagetest.TeardownFunc) {
		st, err := Builder("test-topic", 0)
		if err != nil {
			t.Fatalf("error building storage: %v", err)
		}

		return st, func() {}
	})
}
