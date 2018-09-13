package keyvalue

import (
	"context"
	"fmt"

	"github.com/lovoo/goka/storage"
	"github.com/lovoo/goka/storage/keyvalue/backend"
)

const (
	kilo           = 1024
	mega           = 1024 * kilo
	batchFlushSize = 16 * mega
)

type Storage struct {
	reader
	store         backend.Interface
	currentOffset int64
}

func New(store backend.Interface) *Storage {
	return &Storage{
		reader:        reader{store},
		store:         store,
		currentOffset: -1,
	}
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

	b := s.store.Batch()
	b.Put(idxKeyToValue(bkey), val)   // key -> value
	b.Put(kto, boff)                  // key -> offset
	b.Put(idxOffsetToKey(boff), bkey) // offset -> key

	if oldOffset, err := s.store.Get(kto); err != nil && err != backend.ErrNotFound {
		return err
	} else {
		b.Delete(idxOffsetToKey(oldOffset)) // delete old offset -> key
	}

	return b.Flush()
}

func (s *Storage) DeleteUntil(ctx context.Context, offset int64) (int64, error) {
	iter := s.offsetKeyIterator(0, offset)
	defer iter.Release()

	b := s.store.Batch()
	count := int64(0)

	done := ctx.Done()
	for iter.Next() {
		select {
		case <-done:
			return count, ctx.Err()
		default:
		}

		b.Delete(idxKeyToOffset(iter.Key()))
		b.Delete(idxKeyToValue(iter.Key()))
		b.Delete(iter.Offset())
		count++

		if b.Size() >= batchFlushSize {
			if err := b.Flush(); err != nil {
				return count, err
			}
		}
	}

	return count, b.Flush()
}

func (s *Storage) Delete(key string) error {
	bkey := []byte(key)
	kto := idxKeyToOffset(bkey)

	b := s.store.Batch()
	b.Delete(idxKeyToValue(bkey)) // key -> value
	b.Delete(kto)                 // key -> offset

	if offset, err := s.store.Get(kto); err != nil && err != backend.ErrNotFound {
		return err
	} else {
		b.Delete(idxOffsetToKey(offset)) // offset -> key
	}

	return b.Flush()
}

func (s *Storage) Snapshot() (storage.Snapshot, error) {
	snap, err := s.store.Snapshot()
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		reader:   reader{snap},
		snapshot: snap,
	}, nil
}

func (s *Storage) MarkRecovered() error {
	return s.store.MarkRecovered()
}

func (s *Storage) SetOffset(offset int64) error {
	if offset > s.currentOffset {
		s.currentOffset = offset
	}

	b := s.store.Batch()
	b.Put(keyLocalOffset, marshalOffset(offset))

	return b.Flush()
}

func (s *Storage) Open() error {
	return s.store.Open()
}

func (s *Storage) Close() error {
	return s.store.Close()
}
