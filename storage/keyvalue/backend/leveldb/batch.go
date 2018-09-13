package leveldb

import (
	"github.com/lovoo/goka/storage/keyvalue/backend"
	"github.com/syndtr/goleveldb/leveldb"
)

type batch struct {
	batch   *leveldb.Batch
	storage *Storage
}

func (s *Storage) Batch() backend.Batch {
	return &batch{
		batch:   &leveldb.Batch{},
		storage: s,
	}
}

func (s *batch) Flush() error {
	if err := s.storage.write(s.batch); err != nil {
		return err
	}

	s.batch.Reset()
	return nil
}

func (b *batch) Put(key, value []byte) backend.Batch {
	b.batch.Put(key, value)
	return b
}

func (b *batch) Delete(key []byte) backend.Batch {
	b.batch.Delete(key)
	return b
}

func (b *batch) Size() int {
	return len(b.batch.Dump())
}
