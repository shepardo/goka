package simple

import (
	"sort"
	"sync"

	"github.com/lovoo/goka/storage/keyvalue"
	"github.com/lovoo/goka/storage/keyvalue/backend"
)

type Storage struct {
	m sync.RWMutex

	storage   map[string][]byte
	recovered bool
}

// New returns a new in-memory storage.
func New() *keyvalue.Storage {
	return keyvalue.New(&Storage{
		storage:   make(map[string][]byte),
		recovered: false,
	})
}

func (s *Storage) Snapshot() (backend.Snapshot, error) {
	s.m.RLock()
	snap := make(map[string][]byte, len(s.storage))
	for k, v := range s.storage {
		snap[k] = v
	}
	s.m.RUnlock()

	return &snapshot{storage: snap}, nil
}

func (s *Storage) Batch() backend.Batch {
	return newBatch(s.write)
}

func (s *Storage) write(b *batch) {
	s.m.Lock()
	for _, op := range b.ops {
		op(s.storage)
	}
	s.m.Unlock()
}

func (s *Storage) Has(key []byte) (bool, error) {
	_, has := s.get(string(key))
	return has, nil
}

func (s *Storage) Iterator(start, limit []byte) backend.Iterator {
	var keys []string

	sstart := string(start)
	slimit := string(limit)

	s.m.RLock()
	for k := range s.storage {
		if len(sstart) > 0 && k < sstart {
			// below the range
			continue
		}

		if len(slimit) > 0 && slimit <= k {
			// above the range
			continue
		}

		keys = append(keys, k)
	}
	s.m.RUnlock()

	sort.Strings(keys)

	return &iterator{
		current: -1,
		keys:    keys,
		get:     s.get,
	}
}

func (s *Storage) Get(key []byte) ([]byte, error) {
	val, has := s.get(string(key))
	if !has {
		return nil, backend.ErrNotFound
	}

	return val, nil
}

func (s *Storage) get(key string) ([]byte, bool) {
	s.m.RLock()
	val, has := s.storage[key]
	s.m.RUnlock()
	return val, has
}

func (s *Storage) Set(key []byte, value []byte, offset int64) error {
	s.m.Lock()
	s.storage[string(key)] = value
	s.m.Unlock()
	return nil
}

func (s *Storage) Delete(key []byte) error {
	s.m.Lock()
	delete(s.storage, string(key))
	s.m.Unlock()
	return nil
}

func (s *Storage) MarkRecovered() error {
	return nil
}

func (s *Storage) Open() error {
	return nil
}

func (s *Storage) Close() error {
	return nil
}
