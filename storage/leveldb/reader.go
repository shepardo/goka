package leveldb

import (
	"github.com/lovoo/goka/storage"
	"github.com/syndtr/goleveldb/leveldb"
	ldbiter "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// readDB is the LevelDB read only interface.
type readDB interface {
	Has([]byte, *opt.ReadOptions) (bool, error)
	Get([]byte, *opt.ReadOptions) ([]byte, error)
	NewIterator(*util.Range, *opt.ReadOptions) ldbiter.Iterator
}

// reader implements all the read only functionality. This struct is a helper
// that enables sharing read operation logic between snapshots and the real
// database.
type reader struct {
	db readDB
}

// GetOffset returns the local offset if it is present in the database,
// otherwise it returns the defaul values passed in.
func (r reader) GetOffset(def int64) (int64, error) {
	data, err := r.db.Get(keyLocalOffset, nil)
	if err == leveldb.ErrNotFound {
		return def, nil
	} else if err != nil {
		return 0, err
	}

	return unmarshalOffset(data), nil
}

// Has returns whether the given key exists in the database.
func (r reader) Has(key string) (bool, error) {
	has, err := r.db.Has(idxKeyToValue([]byte(key)), nil)
	if err != nil {
		return false, err
	}

	return has, nil
}

// Get returns the value associated with the given key. If the key does not
// exist, a nil will be returned.
func (r reader) Get(key string) ([]byte, error) {
	val, err := r.db.Get(idxKeyToValue([]byte(key)), nil)
	if err == leveldb.ErrNotFound {
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

	return &Iterator{r.db.NewIterator(&util.Range{rstart, rlimit}, nil)}
}

// offsetToKeyIterator returns a new iterator that iterates over the offset to
// key index. Start and limit define a half-open range [start, limit). If either
// is zero, the range will be unbounded on the respective side.
func (r *reader) offsetKeyIterator(start, limit int64) *offsetKeyIterator {
	rstart, rlimit := Prefix(prefixOffsetToKey)

	if start > 0 {
		rstart = idxOffsetToKey(marshalOffset(start))
	}

	if limit > 0 {
		rlimit = idxOffsetToKey(marshalOffset(limit))
	}

	return &offsetKeyIterator{r.db.NewIterator(
		&util.Range{rstart, rlimit}, nil,
	)}
}
