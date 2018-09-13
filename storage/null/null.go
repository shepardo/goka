package null

import (
	"context"

	"github.com/lovoo/goka/storage"
)

var _ storage.Storage = Storage{}

func Builder(topic string, partition int32) (storage.Storage, error) { return New(), nil }

type Iterator struct{}

func (i Iterator) Next() bool { return false }

func (i Iterator) Error() error { return nil }

func (i Iterator) Key() []byte { return nil }

func (i Iterator) Offset() int64 { return -1 }

func (i Iterator) Value() []byte { return nil }

func (i Iterator) Release() {}

func (i Iterator) Seek([]byte) bool { return false }

type Snapshot struct{ Storage }

func (s Snapshot) Release() {}

type Storage struct{}

func New() Storage { return Storage{} }

func (s Storage) Delete(key string) error { return nil }

func (s Storage) DeleteUntil(ctx context.Context, offset int64) (int64, error) { return 0, nil }

func (s Storage) Has(key string) (bool, error) { return false, nil }

func (s Storage) Get(key string) ([]byte, error) { return nil, nil }

func (s Storage) Set(key string, val []byte, offset int64) error { return nil }

func (s Storage) GetOffset(def int64) (int64, error) { return def, nil }

func (s Storage) SetOffset(offset int64) error { return nil }

func (s Storage) Snapshot() (storage.Snapshot, error) { return Snapshot{}, nil }

func (s Storage) Iterator(start, limit []byte) storage.Iterator {
	return Iterator{}
}

func (s Storage) MarkRecovered() error { return nil }

func (s Storage) Open() error { return nil }

func (s Storage) Close() error { return nil }
