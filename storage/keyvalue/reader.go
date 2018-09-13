package keyvalue

import (
	"github.com/lovoo/goka/storage"
	"github.com/lovoo/goka/storage/keyvalue/backend"
)

// reader implements all the read only functionality. This struct is a helper
// that enables sharing read operation logic between snapshots and the real
// database.
type reader struct {
	backend backend.ReadInterface
}

// GetOffset returns the local offset if it is present in the database,
// otherwise it returns the defaul values passed in.
func (r reader) GetOffset(def int64) (int64, error) {
	data, err := r.backend.Get(keyLocalOffset)
	if err == backend.ErrNotFound {
		return def, nil
	} else if err != nil {
		return 0, err
	}

	return unmarshalOffset(data), nil
}

// Has returns whether the given key exists in the database.
func (r reader) Has(key string) (bool, error) {
	has, err := r.backend.Has(idxKeyToValue([]byte(key)))
	if err != nil {
		return false, err
	}

	return has, nil
}

// Get returns the value associated with the given key. If the key does not
// exist, a nil will be returned.
func (r reader) Get(key string) ([]byte, error) {
	val, err := r.backend.Get(idxKeyToValue([]byte(key)))
	if err == backend.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return val, nil
}

// Iterator returns a new iterator that iterates over the key to value
// index. Start and limit define a half-open range [start, limit]. If either is
// empty, the range will be unbounded on the respective side.
func (r reader) Iterator(start, limit []byte) storage.Iterator {
	rstart, rlimit := Prefix(prefixKeyToValue)

	if len(start) > 0 {
		rstart = idxKeyToValue(start)
	}

	if len(limit) > 0 {
		rlimit = idxKeyToValue(limit)
	}

	return &Iterator{&iterator{r.backend.Iterator(rstart, rlimit)}}
}

// offsetToKeyIterator returns a new iterator that iterates over the offset to
// key index. Start and limit define a half-open range [start, limit). If either
// is zero, the range will be unbounded on the respective side.
func (r *reader) offsetKeyIterator(start, limit int64) *offsetKeyIterator {
	return &offsetKeyIterator{&iterator{r.backend.Iterator(
		idxOffsetToKey(marshalOffset(start)),
		idxOffsetToKey(marshalOffset(limit)),
	)}}
}
