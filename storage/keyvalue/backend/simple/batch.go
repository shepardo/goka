package simple

import (
	"sync"

	"github.com/lovoo/goka/storage/keyvalue/backend"
)

// pre-allocate batch with size of 4 since most batches from the default
// storage implementation include 4 ops
const batchSizeHint = 4

type batchOpFunc func(s map[string][]byte)

type batch struct {
	m   sync.Mutex
	ops []batchOpFunc

	size  int
	write func(*batch)
}

func newBatch(write func(*batch)) *batch {
	return &batch{ops: make([]batchOpFunc, 0, batchSizeHint), write: write}
}

func (b *batch) Put(key, value []byte) backend.Batch {
	skey := string(key)

	b.m.Lock()
	b.ops = append(b.ops, func(s map[string][]byte) {
		s[skey] = value
	})

	b.size = b.size + len(key) + len(value)
	b.m.Unlock()

	return b
}

func (b *batch) Delete(key []byte) backend.Batch {
	skey := string(key)

	b.m.Lock()
	b.ops = append(b.ops, func(s map[string][]byte) {
		delete(s, skey)
	})
	b.m.Unlock()

	b.size = b.size + len(key)

	return b
}

func (b *batch) Flush() error {
	b.write(b)
	return nil
}

// TODO(sami): is the size actually the size of the arguments to put and delete?
func (b *batch) Size() int {
	b.m.Lock()
	size := b.size
	b.m.Unlock()
	return size
}
