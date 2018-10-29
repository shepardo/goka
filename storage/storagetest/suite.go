// Package storagetest implements a test suite for storage implementations.
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

func TestDeleteUntil(t *testing.T, s storage.Storage) {
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatalf("error closing storage: %v", err)
		}
	}()

	st, ok := s.(storage.Cleanable)
	if !ok {
		t.Skip("storage does not support cleaning")
	}

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

	tests := []struct {
		start    []byte
		limit    []byte
		expected []KV
	}{
		{nil, nil, []KV{
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
		{[]byte("key-0"), []byte("key-2"), []KV{
			{"key-0", "off-10"},
			{"key-1", "off-11"},
		}},
		{[]byte("key-8"), nil, []KV{
			{"key-8", "off-8"},
			{"key-9", "off-9"},
		}},
		{nil, []byte("key-2"), []KV{
			{"key-0", "off-10"},
			{"key-1", "off-11"},
		}},
	}

	for _, test := range tests {

		t.Run(fmt.Sprintf("[%s,%s)", test.start, test.limit), func(t *testing.T) {
			iter := st.Iterator(test.start, test.limit)
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

			if err := iter.Error(); err != nil {
				t.Fatalf("unexpected error iterating: %v", err)
			} else if len(expected) > 0 {
				t.Fatalf("expected keys not found: %+v", expected)
			}
		})
	}
}
