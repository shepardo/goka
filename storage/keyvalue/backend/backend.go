package backend

import "errors"

var (
	ErrNotFound = errors.New("not found")
)

type Iterator interface {
	Next() bool
	Error() error
	Key() []byte
	Value() []byte
	Seek([]byte) bool
	Release()
}

type ReadInterface interface {
	Has(key []byte) (bool, error)
	Get(key []byte) ([]byte, error)
	Iterator(start, limit []byte) Iterator
}

type Snapshot interface {
	ReadInterface
	Release()
}

type Batch interface {
	Put([]byte, []byte) Batch
	Delete([]byte) Batch
	Size() int

	Flush() error
}

type Interface interface {
	ReadInterface
	Batch() Batch
	Snapshot() (Snapshot, error)
	MarkRecovered() error
	Open() error
	Close() error
}
