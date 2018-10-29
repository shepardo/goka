// Package null implements a storage that discards everything written into it.
package null

import (
	"context"

	"github.com/lovoo/goka/storage"
)

var _ storage.Storage = Storage{}

// Builder builds a new null storage.
func Builder(topic string, partition int32) (storage.Storage, error) { return New(), nil }

// Iterator is immediately exhausted.
type Iterator struct{}

// Next returns always false.
func (i Iterator) Next() bool { return false }

// Error never returns an error.
func (i Iterator) Error() error { return nil }

// Key is always nil
func (i Iterator) Key() []byte { return nil }

// Offset is always -1.
func (i Iterator) Offset() int64 { return -1 }

// Value is always nil.
func (i Iterator) Value() []byte { return nil }

// Release is a no-op.
func (i Iterator) Release() {}

// Seek returns always false.
func (i Iterator) Seek([]byte) bool { return false }

// Storage discards everything written into it, never finds anything and
// doesn't error.
type Storage struct{}

// New returns a new Storage.
func New() Storage { return Storage{} }

// Delete is no-op.
func (s Storage) Delete(key string) error { return nil }

// DeleteUntil never deletes anything.
func (s Storage) DeleteUntil(ctx context.Context, offset int64) (int64, error) { return 0, nil }

// Has never finds a key.
func (s Storage) Has(key string) (bool, error) { return false, nil }

// Get never finds a key-value pair.
func (s Storage) Get(key string) ([]byte, error) { return nil, nil }

// Set is a no-op.
func (s Storage) Set(key string, val []byte, offset int64) error { return nil }

// GetOffset returns always def.
func (s Storage) GetOffset(def int64) (int64, error) { return def, nil }

// SetOffset is a no-op.
func (s Storage) SetOffset(offset int64) error { return nil }

// Iterator returns an immediately exhausted iterator.
func (s Storage) Iterator(start, limit []byte) storage.Iterator {
	return Iterator{}
}

// MarkRecovered is a no-op
func (s Storage) MarkRecovered() error { return nil }

// Open is a no-op.
func (s Storage) Open() error { return nil }

// Close is a no-op.
func (s Storage) Close() error { return nil }
