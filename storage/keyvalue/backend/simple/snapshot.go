package simple

import (
	"sort"

	"github.com/lovoo/goka/storage/keyvalue/backend"
)

type snapshot struct {
	storage map[string][]byte
}

func (s *snapshot) Has(key []byte) (bool, error) {
	if _, has := s.storage[string(key)]; !has {
		return false, nil
	}

	return true, nil
}

func (s *snapshot) Get(key []byte) ([]byte, error) {
	val, has := s.storage[string(key)]
	if !has {
		return nil, backend.ErrNotFound
	}

	return val, nil
}

func (s *snapshot) Iterator(start, limit []byte) backend.Iterator {
	var keys []string

	sstart := string(start)
	slimit := string(limit)

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

	sort.Strings(keys)

	return &iterator{
		current: -1,
		keys:    keys,
		get: func(key string) ([]byte, bool) {
			val, has := s.storage[key]
			return val, has
		},
	}
}

func (s *snapshot) Release() {}
