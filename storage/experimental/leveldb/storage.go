// Package leveldb implements a cleanable LevelDB storage
package leveldb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	kilo           = 1024
	mega           = 1024 * kilo
	batchFlushSize = 16 * mega
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

func (s *Storage) get(key []byte) ([]byte, error) {
	if s.tx != nil {
		return s.tx.Get(key, nil)
	}

	return s.db.Get(key, nil)
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

	return unmarshalOffset(data), nil
}

// Has returns whether the given key exists in the database.
func (s *Storage) Has(key string) (bool, error) {
	has := s.db.Has
	if s.tx != nil {
		has = s.tx.Has
	}

	return has(idxKeyToValue([]byte(key)), nil)
}

// Get returns the value associated with the given key. If the key does not
// exist, a nil will be returned.
func (s *Storage) Get(key string) ([]byte, error) {
	val, err := s.get(idxKeyToValue([]byte(key)))
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
func (s *Storage) Iterator(start, limit []byte) storage.Iterator {
	rstart, rlimit := storage.Prefix(prefixKeyToValue)

	if len(start) > 0 {
		rstart = idxKeyToValue(start)
	}

	if len(limit) > 0 {
		rlimit = idxKeyToValue(limit)
	}

	newIterator := s.db.NewIterator
	if s.tx != nil {
		newIterator = s.tx.NewIterator
	}

	return &Iterator{newIterator(&util.Range{rstart, rlimit}, nil)}
}

// offsetToKeyIterator returns a new iterator that iterates over the offset to
// key index. Start and limit define a half-open range [start, limit). If either
// is zero, the range will be unbounded on the respective side.
func (s *Storage) offsetKeyIterator(start, limit int64) *offsetKeyIterator {
	rstart, rlimit := storage.Prefix(prefixOffsetToKey)

	if start > 0 {
		rstart = idxOffsetToKey(marshalOffset(start))
	}

	if limit > 0 {
		rlimit = idxOffsetToKey(marshalOffset(limit))
	}

	newIterator := s.db.NewIterator
	if s.tx != nil {
		newIterator = s.tx.NewIterator
	}

	return &offsetKeyIterator{newIterator(
		&util.Range{rstart, rlimit}, nil,
	)}
}

func (s *Storage) Set(key string, val []byte, offset int64) error {
	if offset < 0 {
		return fmt.Errorf("negative offset: %v", offset)
	}

	bkey := []byte(key)
	boff := marshalOffset(offset)
	kto := idxKeyToOffset(bkey)

	b := &leveldb.Batch{}
	b.Put(kto, boff)                  // key -> offset
	b.Put(idxOffsetToKey(boff), bkey) // offset -> key
	b.Put(idxKeyToValue(bkey), val)   // key -> value

	if s.tx != nil {
		return s.tx.Write(b, nil)
	}

	if oldOffset, err := s.db.Get(kto, nil); err != nil && err != leveldb.ErrNotFound {
		return err
	} else {
		b.Delete(idxOffsetToKey(oldOffset)) // delete old offset -> key
	}

	return s.db.Write(b, nil)
}

func (s *Storage) DeleteUntil(ctx context.Context, offset int64) (int64, error) {
	iter := s.offsetKeyIterator(0, offset)
	defer iter.Release()

	b := &leveldb.Batch{}
	count := int64(0)

	done := ctx.Done()
	for iter.Next() {
		select {
		case <-done:
			return count, ctx.Err()
		default:
		}

		b.Delete(idxKeyToOffset(iter.Key()))
		b.Delete(idxOffsetToKey(iter.Offset()))
		b.Delete(idxKeyToValue(iter.Key()))
		count++

		if len(b.Dump()) >= batchFlushSize {
			if s.tx != nil {
				if err := s.tx.Write(b, nil); err != nil {
					return count, err
				}

				b.Reset()
				continue
			}

			if err := s.db.Write(b, nil); err != nil {
				return count, err
			}

			b.Reset()
		}
	}

	if s.tx != nil {
		if err := s.tx.Write(b, nil); err != nil {
			return count, err
		}

		return count, nil
	}

	return count, s.db.Write(b, nil)
}

func (s *Storage) Delete(key string) error {
	bkey := []byte(key)
	kto := idxKeyToOffset(bkey)

	b := &leveldb.Batch{}
	b.Delete(kto)                 // key -> offset
	b.Delete(idxKeyToValue(bkey)) // key -> value

	if s.tx != nil {
		return s.tx.Write(b, nil)
	}

	if offset, err := s.db.Get(kto, nil); err != nil && err != leveldb.ErrNotFound {
		return err
	} else {
		b.Delete(idxOffsetToKey(offset)) // offset -> key
	}

	return s.db.Write(b, nil)
}

func (s *Storage) pruneOffsets() error {
	oldestPruned := int64(0)

	val, err := s.db.Get(keyOldestPrunedOffset, nil)
	if err != nil {
		if err != leveldb.ErrNotFound {
			return err
		}
	} else {
		oldestPruned = unmarshalOffset(val)
	}

	iter := s.offsetKeyIterator(oldestPruned, 0)
	defer iter.Release()

	b := &leveldb.Batch{}

	for iter.Next() {
		deleted := false
		old := iter.Offset()

		current, err := s.db.Get(idxKeyToOffset(iter.Key()), nil)
		if err == leveldb.ErrNotFound {
			deleted = true
		} else if err != nil {
			return err
		}

		if bytes.Equal(old, current) && !deleted {
			continue
		}

		b.Delete(idxOffsetToKey(old))

		if len(b.Dump()) >= batchFlushSize {
			if err := s.db.Write(b, nil); err != nil {
				return err
			}

			b.Reset()
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	b.Put(keyOldestPrunedOffset, marshalOffset(s.currentOffset))
	return s.db.Write(b, nil)
}

func (s *Storage) MarkRecovered() error {
	if err := s.tx.Commit(); err != nil {
		return err
	}

	s.tx = nil

	return s.pruneOffsets()
}

func (s *Storage) SetOffset(offset int64) error {
	if offset < s.currentOffset {
		return nil
	}

	boff := marshalOffset(offset)

	b := &leveldb.Batch{}
	b.Put(keyLocalOffset, boff)

	if s.tx != nil {
		return s.tx.Write(b, nil)
	}

	s.currentOffset = offset
	b.Put(keyOldestPrunedOffset, boff)
	return s.db.Write(b, nil)
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
