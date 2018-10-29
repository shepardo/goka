package leveldb

import "encoding/binary"

var (
	// keyLocalOffset is the key under which the local processed offset is stored.
	keyLocalOffset = []byte{'L'}

	// keyOldestPrunedOffset stores the oldest pruned offset to avoid pruning
	// whole storage on every recovery.
	keyOldestPrunedOffset = []byte{'P'}

	// prefixKeyToValue denotes the prefix of the key to value index.
	prefixKeyToValue = []byte{'V'}
	// prefixOffsetToKey denotes the prefix of the offset to key index.
	prefixOffsetToKey = []byte{'O'}
	// prefixKeyToOffset denotes the prefix of the key to offset index.
	prefixKeyToOffset = []byte{'K'}
)

func buildKey(prefix, suffix []byte) []byte {
	b := make([]byte, len(prefix)+len(suffix))
	copy(b, prefix)
	copy(b[len(prefix):], suffix)
	return b
}

// idxKeyToValue returns a key that locates the value in the key to value index.
func idxKeyToValue(key []byte) []byte {
	return buildKey(prefixKeyToValue, key)
}

// idxOffsetToKey returns a key that locates the offset in the offset to key index.
func idxOffsetToKey(offset []byte) []byte {
	return buildKey(prefixOffsetToKey, offset)
}

// idxKeyToOffset returns a key that locates the key in the key to offset index.
func idxKeyToOffset(key []byte) []byte {
	return buildKey(prefixKeyToOffset, key)
}

// marshalOffset returns a serialized offset. It uses fixed size big endian
// encoding to keep the offsets lexicographically sortable.
func marshalOffset(offset int64) []byte {
	b := make([]byte, 64/8) // int64 size
	binary.BigEndian.PutUint64(b, uint64(offset))
	return b
}

// unmarshalOffset returns a deserialized offset.
func unmarshalOffset(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
