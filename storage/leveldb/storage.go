package leveldb

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/lovoo/goka/storage"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	kilo           = 1024
	mega           = 1024 * kilo
	batchFlushSize = 16 * mega
)

type db interface {
	Get([]byte, *opt.ReadOptions) ([]byte, error)

	Put([]byte, *opt.WriteOptions) error
	Delete([]byte, *opt.WriteOptions) error
}

type Storage struct {
	reader

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
		reader: reader{tx},

		tx: tx,
		db: db,

		currentOffset: -1,
	}, nil
}

// https://github.com/syndtr/goleveldb/blob/master/leveldb/util/range.go#L20
func Prefix(prefix []byte) ([]byte, []byte) {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return prefix, limit
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
	iter := s.reader.offsetKeyIterator(0, offset)
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

func (s *Storage) Snapshot() (storage.Snapshot, error) {
	snap, err := s.db.GetSnapshot()
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		reader: reader{snap},
		snap:   snap,
	}, nil
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
	log.Printf("oldest pruned: %v", s.currentOffset)
	return s.db.Write(b, nil)
}

func (s *Storage) MarkRecovered() error {
	began := time.Now()
	if err := s.tx.Commit(); err != nil {
		return err
	}
	commitFinished := time.Since(began)

	s.reader = reader{s.db}
	s.tx = nil

	began = time.Now()
	defer func() {
		log.Printf("commit time: %v, pruning time: %v", commitFinished, time.Since(began))
	}()
	log.Printf("pruning offsets...")
	return s.pruneOffsets()
}

func (s *Storage) SetOffset(offset int64) error {
	if offset > s.currentOffset {
		s.currentOffset = offset
	}

	boff := marshalOffset(offset)

	b := &leveldb.Batch{}
	b.Put(keyLocalOffset, boff)

	if s.tx != nil {
		return s.tx.Write(b, nil)
	}

	b.Put(keyOldestPrunedOffset, boff)
	return s.db.Write(b, nil)
}

func (s *Storage) Open() error {
	return nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}
