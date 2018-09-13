package leveldb

import (
	"fmt"

	"github.com/lovoo/goka/storage/keyvalue"
	"github.com/lovoo/goka/storage/keyvalue/backend"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// reads is the LevelDB read only interface.
type reads interface {
	Has([]byte, *opt.ReadOptions) (bool, error)
	Get([]byte, *opt.ReadOptions) ([]byte, error)
	NewIterator(*util.Range, *opt.ReadOptions) iterator.Iterator
}

type Storage struct {
	readSupport

	tx *leveldb.Transaction
	db *leveldb.DB
}

func New(db *leveldb.DB) (*keyvalue.Storage, error) {
	tx, err := db.OpenTransaction()
	if err != nil {
		return nil, err
	}

	return keyvalue.New(&Storage{
		readSupport: readSupport{tx},

		tx: tx,
		db: db,
	}), nil
}

func (s *Storage) MarkRecovered() error {
	if s.recovered() {
		return fmt.Errorf("storage marked recovered twice")
	}

	err := s.tx.Commit()
	s.tx = nil
	s.readSupport.db = s.db

	return err
}

func (s *Storage) write(b *leveldb.Batch) error {
	if !s.recovered() {
		return s.tx.Write(b, nil)
	}

	return s.db.Write(b, nil)
}

func (s *Storage) recovered() bool {
	return s.tx == nil
}

func (s *Storage) Open() error {
	return nil
}

func (s *Storage) Close() error {
	// TODO(sami): catch errors if both fail
	if !s.recovered() {
		if err := s.tx.Commit(); err != nil {
			return fmt.Errorf("error closing transaction: %v", err)
		}
	}

	return s.db.Close()
}

func (s *Storage) Snapshot() (backend.Snapshot, error) {
	if !s.recovered() {
		return nil, fmt.Errorf("storage must not be snapshotted before recovery")
	}

	snap, err := s.db.GetSnapshot()
	if err != nil {
		return nil, err
	}

	return newSnapshot(snap), nil
}
