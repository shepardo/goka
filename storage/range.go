package storage

// Prefix returns start and limit of a range that can be used to iterate over a
// prefix.
//
// Adapted from https://github.com/syndtr/goleveldb/blob/master/leveldb/util/range.go#L20
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
