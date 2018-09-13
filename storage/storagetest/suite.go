package storagetest

import (
	"context"
	"fmt"
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/lovoo/goka/storage"
)

type TeardownFunc func()

type Builder func(*testing.T) (storage.Storage, TeardownFunc)

type Iterable interface {
	Iterator(start, limit []byte) storage.Iterator
}

func ensureHas(t *testing.T, st storage.Storage, key string) {
	has, err := st.Has(key)
	ensure.Nil(t, err)
	ensure.True(t, has)
}

func ensureHasNot(t *testing.T, st storage.Storage, key string) {
	has, err := st.Has(key)
	ensure.Nil(t, err)
	ensure.False(t, has)
}

func ensureSet(t *testing.T, st storage.Storage, key string, val string, off int64) {
	ensure.Nil(t, st.Set(key, []byte(val), off))
}

func ensureGet(t *testing.T, st storage.Storage, key string) []byte {
	val, err := st.Get(key)
	ensure.Nil(t, err)
	return val
}

func ensureDelete(t *testing.T, st storage.Storage, key string) {
	ensure.Nil(t, st.Delete(key))
}

func ensureGetNotFound(t *testing.T, st storage.Storage, key string) {
	val, err := st.Get(key)
	ensure.True(t, len(val) == 0)
	ensure.Nil(t, err)
}

func Suite(t *testing.T, build Builder) {
	tests := []struct {
		name string
		run  func(t *testing.T, st storage.Storage)
	}{
		{"CRUD", TestCRUD},
		{"Snapshot", TestSnapshot},
		{"Iteration", TestIteration},
		{"Delete Until", TestDeleteUntil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			st, teardown := build(t)
			defer teardown()

			test.run(t, st)
		})
	}
}

func TestDeleteUntil(t *testing.T, st storage.Storage) {
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("error closing storage: %v", err)
		}
	}()

	PopulateStorage(t, st, []KVO{
		{KV{"key-0", "off-0"}, 0},
		{KV{"key-1", "off-1"}, 1},
		{KV{"key-2", "off-3"}, 2},
		{KV{"key-3", "off-3"}, 3},
		{KV{"key-4", "off-4"}, 4},
		{KV{"key-5", "off-5"}, 5},
		{KV{"key-6", "off-6"}, 6},
		{KV{"key-7", "off-7"}, 7},
		{KV{"key-8", "off-8"}, 8},
		{KV{"key-9", "off-9"}, 9},
		{KV{"key-0", "off-10"}, 10},
		{KV{"key-1", "off-11"}, 11},
		{KV{"key-2", "off-12"}, 12},
		{KV{"key-3", "off-13"}, 13},
		{KV{"key-4", "off-14"}, 14},
	})

	if err := st.MarkRecovered(); err != nil {
		t.Fatalf("error marking storage recovered: %v", err)
	}

	cnt, err := st.DeleteUntil(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cnt != 5 {
		t.Fatalf("expected 5 records to be deleted, actual was %v", cnt)
	}

	ContainsInOrder(t, st, []KV{
		{"key-0", "off-10"},
		{"key-1", "off-11"},
		{"key-2", "off-12"},
		{"key-3", "off-13"},
		{"key-4", "off-14"},
	})
}

func TestCRUD(t *testing.T, st storage.Storage) {
	key := "key-1"
	val := "val-1"

	ensureHasNot(t, st, key)
	ensureGetNotFound(t, st, key)
	ensureSet(t, st, key, val, 0)
	ensureHas(t, st, key)
	b := ensureGet(t, st, key)
	ensure.DeepEqual(t, b, []byte(val))
	ensureDelete(t, st, key)
	ensureHasNot(t, st, key)

	ensure.Nil(t, st.Close())
}

func TestSnapshot(t *testing.T, st storage.Storage) {
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("error closing storage: %v", err)
		}
	}()

	if err := st.MarkRecovered(); err != nil {
		t.Fatalf("unexpected error marking storage recovered: %v", err)
	}

	if err := st.Set("key-1", []byte("val-1"), 0); err != nil {
		t.Fatalf("unexpected error setting: %v", err)
	}

	if has, err := st.Has("key-1"); err != nil {
		t.Fatalf("unexpected error checking key existence: %v", err)
	} else if !has {
		t.Fatalf("expected key-1 to exist")
	}

	snap, err := st.Snapshot()
	if err != nil {
		t.Fatalf("unexpected error snapshotting: %v", err)
	}
	defer snap.Release()

	// update key
	if err := st.Set("key-1", []byte("val-2"), 1); err != nil {
		t.Fatalf("unexpected error updating key: %v", err)
	}

	if val, err := st.Get("key-1"); err != nil {
		t.Fatalf("error getting from storage: %v", err)
	} else if string(val) != "val-2" {
		t.Fatalf("expected to get updated value from storage, got %v", string(val))
	}

	if val, err := snap.Get("key-1"); err != nil {
		t.Fatalf("error getting from snapshot: %v", err)
	} else if string(val) != "val-1" {
		t.Fatalf("expected snapshot to contain old value, got %v", string(val))
	}

	// delete key
	if err := st.Delete("key-1"); err != nil {
		t.Fatalf("error deleting key: %v", err)
	}

	if has, err := st.Has("key-1"); err != nil {
		t.Fatalf("error checking key existence in storage: %v", err)
	} else if has {
		t.Fatalf("expected key to be deleted in storage: %v", err)
	}

	if has, err := snap.Has("key-1"); err != nil {
		t.Fatalf("error checking key existence in snapshot: %v", err)
	} else if !has {
		t.Fatalf("expected key to be preset in snapshot after deletion")
	}
}

func TestIteration(t *testing.T, st storage.Storage) {
	defer func() {
		if err := st.Close(); err != nil {
			t.Fatalf("error closing storage: %v", err)
		}
	}()

	if err := st.MarkRecovered(); err != nil {
		t.Fatalf("error marking storage recovered: %v", err)
	}

	// populate with storage with 10 keys with 5 of them updated with with a later
	// offset
	for i := int64(0); i < 15; i++ {
		if err := st.Set(fmt.Sprintf("key-%v", i%10), []byte(fmt.Sprintf("off-%v", i)), i); err != nil {
			t.Fatalf("error setting: %v", err)
		}
	}

	t.Run("Storage", func(t *testing.T) { TestIterators(t, st) })

	snap, err := st.Snapshot()
	if err != nil {
		t.Fatalf("error snapshotting storage: %v", err)
	}
	defer snap.Release()

	// delete all values, snapshot should still iterate as expected
	for i := 0; i < 10; i++ {
		if err := st.Delete(fmt.Sprintf("key-%v", i)); err != nil {
			t.Fatalf("error deleting: %v", err)
		}
	}

	t.Run("Snapshot", func(t *testing.T) { TestIterators(t, snap) })
}

func TestIterators(t *testing.T, st storage.ReadInterface) {
	t.Run("Key to Value", func(t *testing.T) {
		TestIterator(t, st)
	})
}

func TestIterator(t *testing.T, st storage.ReadInterface) {
	tests := []struct {
		start    string
		limit    string
		expected []KV
	}{
		{"", "", []KV{
			{"key-0", "off-10"},
			{"key-1", "off-11"},
			{"key-2", "off-12"},
			{"key-3", "off-13"},
			{"key-4", "off-14"},
			{"key-5", "off-5"},
			{"key-6", "off-6"},
			{"key-7", "off-7"},
			{"key-8", "off-8"},
			{"key-9", "off-9"},
		}},
		{"key-0", "key-2", []KV{
			{"key-0", "off-10"},
			{"key-1", "off-11"},
		}},
		{"key-8", "", []KV{
			{"key-8", "off-8"},
			{"key-9", "off-9"},
		}},
		{"", "key-2", []KV{
			{"key-0", "off-10"},
			{"key-1", "off-11"},
		}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("[%s,%s)", test.start, test.limit), func(t *testing.T) {
			iter := st.Iterator([]byte(test.start), []byte(test.limit))
			defer iter.Release()

			expected := test.expected

			for iter.Next() {
				key := string(iter.Key())
				value := string(iter.Value())

				exp := expected[0]

				if key != exp.Key || value != exp.Value {
					t.Fatalf("expected %v:%v, got %v:%v", exp.Key, exp.Value, key, value)
				}

				expected = expected[1:]
			}

			if len(expected) > 0 {
				t.Fatalf("expected keys not found: %+v", expected)
			} else if iter.Next() {
				t.Fatalf("expected iterator to be exhausted but it was not")
			}
		})
	}
}
