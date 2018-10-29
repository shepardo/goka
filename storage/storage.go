// Package storage defines the storage interface of Goka and provides utilities.
package storage

import "context"

// Builder creates a local storage (a persistent cache) for a topic
// table. Builder creates one storage for each partition of the topic.
type Builder func(topic string, partition int32) (Storage, error)

// Iterator iterates over key-value pairs.
type Iterator interface {
	// Next moves the iterator to the next key-value pair and whether such a pair
	// exists. Caller should check for possible error by calling Error after Next
	// returns false.
	Next() bool
	// Error returns the error that stopped the iteration if any.
	Error() error
	// Release frees the resources associated with the iterator.
	Release()
	// Key returns the current key. Caller should not keep references to the
	// buffer or modify its contents.
	Key() []byte
	// Value returns the current value. Caller should not keep references to the
	// buffer or modify its contents.
	Value() []byte
	// Seek moves the iterator to the begining of a key-value pair sequence that
	// is greater or equal to the given key. It returns whether at least one of
	// such key-value pairs exist. Next must be called after seeking to access
	// the first pair.
	Seek(key []byte) bool
}

// Storage is the interface Goka expects from a storage implementation.
// Implementations of this interface must be safe for any number of concurrent
// readers with one writer.
type Storage interface {
	// Has returns whether the given key exists in the database.
	Has(key string) (bool, error)
	// Get returns the value associated with the given key. If the key does not
	// exist, a nil will be returned.
	Get(key string) ([]byte, error)
	// Iterator returns a new iterator that iterates over the key-value
	// pairs. Start and limit define a half-open range [start, limit). If either
	// is nil, the range will be unbounded on the respective side.
	Iterator(start, limit []byte) Iterator
	// GetOffset gets the local offset of the storage.
	GetOffset(def int64) (int64, error)
	// Set stores a key-value pair.
	Set(key string, value []byte, offset int64) error
	// Delete deletes a key-value pair from the storage.
	Delete(key string) error
	// SetOffset sets the local offset of the storage.
	SetOffset(offset int64) error

	// MarkRecovered marks the storage as recovered. Recovery message throughput
	// can be a lot higher than during normal operation. This can be used to switch
	// to a different configuration after the recovery is done.
	MarkRecovered() error

	Open() error

	// Close closes the storage.
	Close() error
}

// Cleanable is the interface of a Storage implementation that support cleaning.
type Cleanable interface {
	Storage
	// DeleteUntil deletes every key that corresponds to a lower offset than the
	// one given.
	DeleteUntil(ctx context.Context, offset int64) (int64, error)
}
