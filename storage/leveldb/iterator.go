package leveldb

import (
	"bytes"

	ldbiter "github.com/syndtr/goleveldb/leveldb/iterator"
)

type iterator struct {
	ldbiter.Iterator
}

func (i iterator) Next() bool {
	next := i.Iterator.Next()
	if next && bytes.Equal(i.Key(), keyLocalOffset) {
		next = i.Iterator.Next()
	}

	return next
}
