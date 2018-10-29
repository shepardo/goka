// Package leveldb implements a LevelDB storage.
package leveldb

import (
	"fmt"
	"strconv"

	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	// keyLocalOffset is the key under which the local processed offset is stored.
	keyLocalOffset = []byte("__offset")
)

type Storage struct {
	currentOffset int64

	tx *leveldb.Transaction
	db *leveldb.DB
}

func New(db *leveldb.DB) (*Storage, error) {
	tx, err := db.OpenTransaction()
	if err != nil {
		return nil, err
	}

	return &Storage{
		tx: tx,
		db: db,

		currentOffset: -1,
	}, nil
}

// GetOffset returns the local offset if it is present in the database,
// otherwise it returns the defaul values passed in.
func (s *Storage) GetOffset(def int64) (int64, error) {
	data, err := s.get(keyLocalOffset)
	if err == leveldb.ErrNotFound {
		return def, nil
	} else if err != nil {
		return 0, err
	}

	value, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error decoding offset: %v", err)
	}

	return value, nil
}

// Has returns whether the given key exists in the database.
func (s *Storage) Has(key string) (bool, error) {
	bkey := []byte(key)

	if s.tx != nil {
		return s.tx.Has(bkey, nil)
	}

	return s.db.Has(bkey, nil)
}

func (s *Storage) get(key []byte) ([]byte, error) {
	if s.tx != nil {
		return s.tx.Get(key, nil)
	}

	return s.db.Get(key, nil)
}

// Get returns the value associated with the given key. If the key does not
// exist, a nil will be returned.
func (s *Storage) Get(key string) ([]byte, error) {
	val, err := s.get([]byte(key))
	if err == leveldb.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return val, nil
}

// Iterator returns a new iterator that iterates over the key to value
// index. Start and limit define a half-open range [start, limit]. If either is
// nil, the range will be unbounded on the respective side.
func (s *Storage) Iterator(start, limit []byte) storage.Iterator {
	r := &util.Range{start, limit}
	if s.tx != nil {
		return iterator{s.tx.NewIterator(r, nil)}
	}

	return iterator{s.db.NewIterator(r, nil)}
}

func (s *Storage) put(key, val []byte) error {
	if s.tx != nil {
		return s.tx.Put(key, val, nil)
	}

	return s.db.Put(key, val, nil)
}

func (s *Storage) Set(key string, val []byte, offset int64) error {
	return s.put([]byte(key), val)
}

func (s *Storage) Delete(key string) error {
	bkey := []byte(key)

	if s.tx != nil {
		return s.tx.Delete(bkey, nil)
	}

	return s.db.Delete(bkey, nil)
}

func (s *Storage) MarkRecovered() error {
	if err := s.tx.Commit(); err != nil {
		return err
	}

	s.tx = nil

	return nil
}

func (s *Storage) SetOffset(offset int64) error {
	if offset > s.currentOffset {
		return nil
	}

	s.currentOffset = offset
	return s.put(keyLocalOffset, []byte(strconv.FormatInt(offset, 10)))
}

func (s *Storage) Open() error {
	return nil
}

func (s *Storage) Close() error {
	errs := &multierr.Errors{}
	if s.tx != nil {
		errs.Collect(s.tx.Commit())
	}
	errs.Collect(s.db.Close())
	return errs.NilOrError()
}
