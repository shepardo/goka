package keyvalue

import (
	"bytes"

	"github.com/lovoo/goka/storage/keyvalue/backend"
)

// iterator is a base iterator that provides some of the common functionality.
type iterator struct {
	iter backend.Iterator
}

// Next advances the iterator to the next key.
func (i *iterator) Next() bool {
	return i.iter.Next()
}

// Releases releases the iterator and the associated snapshot. The iterator is
// not usable anymore after calling Release.
func (i *iterator) Release() {
	i.iter.Release()
}

func (i *iterator) Error() error {
	return i.Error()
}

// Iterator iterates over the key-value pairs
type Iterator struct {
	*iterator
}

// Key returns the current iterated key.
func (i *Iterator) Key() []byte {
	return bytes.TrimPrefix(i.iter.Key(), prefixKeyToValue)
}

// Value returns the value associated with the current key.
func (i *Iterator) Value() []byte {
	return i.iter.Value()
}

// Seek moves the iterator to the position of the given key.
func (i *Iterator) Seek(key []byte) bool {
	return i.iter.Seek(idxKeyToValue(key))
}

// offsetKeyIterator iterates over the offset to key index.
type offsetKeyIterator struct {
	*iterator
}

// Offset returns the current iterated offset's complete index key.
func (i *offsetKeyIterator) Offset() []byte {
	return i.iter.Key()
}

// Key returns the key the current offset is associated with.
func (i *offsetKeyIterator) Key() []byte {
	return i.iter.Value()
}

// Seek moves the iterator to the specified offset and returns whether the
// location was found.
func (i *offsetKeyIterator) Seek(offset int64) bool {
	return i.iter.Seek(idxOffsetToKey(marshalOffset(offset)))
}
