package storage

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

type ConstIterator struct {
	current int
	keys    [][]byte
	values  [][]byte

	err error
}

func (c *ConstIterator) Len() int           { return len(c.keys) }
func (c *ConstIterator) Less(i, j int) bool { return bytes.Compare(c.keys[i], c.keys[j]) == -1 }
func (c *ConstIterator) Swap(i, j int) {
	c.keys[i], c.keys[j] = c.keys[j], c.keys[i]
	c.values[i], c.values[j] = c.values[j], c.values[i]
}

func NewConstIterator(kv map[string]string) *ConstIterator {
	keys := make([][]byte, 0, len(kv))
	vals := make([][]byte, 0, len(kv))

	for k, v := range kv {
		keys = append(keys, []byte(k))
		vals = append(vals, []byte(v))
	}

	c := &ConstIterator{current: -1, keys: keys, values: vals}

	sort.Sort(c)

	return c
}

func (c *ConstIterator) Next() bool {
	if c.err != nil {
		return false
	}

	c.current++
	return c.current < len(c.keys)
}

func (c *ConstIterator) Error() error {
	return c.err
}

func (c *ConstIterator) Key() []byte {
	return c.keys[c.current]
}

func (c *ConstIterator) Value() []byte {
	return c.values[c.current]
}

func (c *ConstIterator) Seek(key []byte) bool {
	if c.err != nil {
		return false
	}

	for i := range c.keys {
		if bytes.Compare(c.keys[i], key) >= 0 {
			c.current = i - 1
			return true
		}
	}

	return false
}

func (c *ConstIterator) Release() {}

func TestMergeIterator(t *testing.T) {
	iter := MergeIterator([]Iterator{
		NewConstIterator(map[string]string{
			"key-0": "val-0",
			"key-3": "val-3",
		}),
		NewConstIterator(map[string]string{
			"key-1": "val-1",
			"key-4": "val-4",
			"key-6": "val-6",
			"key-7": "val-7",
			"key-8": "val-8",
		}),
		NewConstIterator(map[string]string{}),
		NewConstIterator(map[string]string{
			"key-2": "val-2",
			"key-5": "val-5",
			"key-9": "val-9",
		}),
	})

	t.Run("Ordered Iteration", func(t *testing.T) {
		testOrderedIteration(t, iter)
	})

	t.Run("Seek on Exhausted", func(t *testing.T) {
		if !iter.Seek([]byte("key-0")) {
			t.Fatalf("expected seek to succeed")
		}

		testOrderedIteration(t, iter)
	})

	t.Run("No Iterators", func(t *testing.T) {
		iter := MergeIterator(nil)
		if iter.Next() {
			t.Fatalf("expected an empty merge iterator to return false on next")
		}

		if iter.Seek(nil) {
			t.Fatalf("expected an empty merge iterator failing to seek")
		}
	})
}

func testOrderedIteration(t *testing.T, iter Iterator) {
	counter := 0
	for ; iter.Next(); counter++ {
		key := iter.Key()
		expected := fmt.Sprintf("key-%v", counter)
		if bytes.Compare(key, []byte(expected)) != 0 {
			t.Errorf("received wrong key %s, expected: %s", string(key), expected)
		}
	}

	if counter < 10 {
		t.Fatalf("expected to have iterated 10 pairs, actually got %v", counter)
	}
}
