package storagetest

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/lovoo/goka/storage"
)

type KV struct {
	Key   string
	Value string
}

type KVO struct {
	KV
	Offset int64
}

func TempDir(t *testing.T, dir, prefix string) (string, TeardownFunc) {
	t.Helper()

	name, err := ioutil.TempDir(dir, prefix)
	if err != nil {
		t.Fatalf("error creating temporary directory: %v", err)
	}

	return name, func() {
		if err := os.RemoveAll(name); err != nil {
			t.Fatalf("error removing temporary directory: %v", err)
		}
	}
}

func PopulateStorage(t *testing.T, st storage.Storage, vals []KVO) {
	t.Helper()

	for _, v := range vals {
		if err := st.Set(v.Key, []byte(v.Value), v.Offset); err != nil {
			t.Fatalf("error populating storage: %v", err)
		}
	}
}

func ClearStorage(t *testing.T, st storage.Storage) {
	t.Helper()

	iter := st.Iterator(nil, nil)
	iter.Release()

	for iter.Next() {
		if err := st.Delete(string(iter.Key())); err != nil {
			t.Fatalf("error clearing storage: %v", err)
		}
	}
}

func ContainsInOrder(t *testing.T, st storage.Storage, vals []KV) {
	t.Helper()

	iter := st.Iterator(nil, nil)
	defer iter.Release()

	for i := 0; iter.Next(); i++ {
		if i >= len(vals) {
			t.Fatalf("storage contains more elements than the expected %v", len(vals))
		}

		actual := vals[i]
		expected := KV{string(iter.Key()), string(iter.Value())}

		if actual != expected {
			t.Fatalf("storage returned an unexpected value: expected: %+v, actual: %+v", expected, actual)
		}
	}
}
