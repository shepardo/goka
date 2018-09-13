package storage

import "context"

// Builder creates a local storage (a persistent cache) for a topic
// table. Builder creates one storage for each partition of the topic.
type Builder func(topic string, partition int32) (Storage, error)

// Iterator iterates over key-value pairs.
type Iterator interface {
	// Next moves the iterator to the next pair and returns whether another pair
	// exists.
	Next() bool
	// Error returns whether the iterator stopped due to an error
	Error() error
	// Release releases the iterator.
	Release()
	// Key returns the current key.
	Key() []byte
	// Value returns the value associated with the key
	Value() []byte
	// Seek moves the iterator to the first key-value pair that is greater or equal
	// to the given key. It returns whether such pair exists.
	Seek([]byte) bool
}

// ReadInterface contains the read only parts of a storage.
type ReadInterface interface {
	// Has returns whether the given key exists in the database.
	Has(key string) (bool, error)
	// Get returns the value associated with the given key. If the key does not
	// exist, a nil will be returned.
	Get(key string) ([]byte, error)

	// Iterator returns a new iterator that iterates over the key-value
	// pairs. Start and limit define a half-open range [start, limit]. If either
	// is empty, the range will be unbounded on the respective side.
	Iterator([]byte, []byte) Iterator

	// GetOffset gets the local offset of the storage.
	GetOffset(def int64) (int64, error)
}

// Snapshot is the interface of a storage snapshot.
type Snapshot interface {
	ReadInterface
	// Release releases the snapshot and frees the associated resource.
	Release()
}

// Storage is the interface Goka expects from a storage implementation.
// Implementations of this interface must be safe for any number of concurrent
// readers with one writer.
type Storage interface {
	ReadInterface
	// Set stores a key-value pair.
	Set(key string, val []byte, offset int64) error
	// Delete deletes a key-value pair from the storage.
	Delete(key string) error
	// DeleteUntil deletes every key that corresponds to a lower offset than the
	// one given.
	DeleteUntil(ctx context.Context, offset int64) (int64, error)
	// SetOffset sets the local offset of the storage.
	SetOffset(offset int64) error

	// Snapshot returns a snapshot of the storage's current state.
	Snapshot() (Snapshot, error)

	// MarkRecovered marks the storage as recovered. Recovery message throughput
	// can be a lot higher than during normal operation. This can be used to switch
	// to a different configuration after the recovery is done.
	MarkRecovered() error

	Open() error

	// Close closes the storage.
	Close() error
}
