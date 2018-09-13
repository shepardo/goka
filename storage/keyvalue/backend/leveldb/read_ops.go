package leveldb

import (
	"github.com/lovoo/goka/storage/keyvalue/backend"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// readSupport implements LevelDB read operations. This is a separate struct for
// embedding into Storage and Snapshot objects.
type readSupport struct {
	db reads
}

func (r *readSupport) Iterator(start, limit []byte) backend.Iterator {
	return r.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
}

func (r *readSupport) Has(key []byte) (bool, error) {
	return r.db.Has(key, nil)
}

func (r *readSupport) Get(key []byte) ([]byte, error) {
	val, err := r.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, backend.ErrNotFound
	} else if err != nil {
		return nil, err
	}

	return val, nil
}
