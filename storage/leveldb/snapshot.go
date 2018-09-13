package leveldb

import "github.com/syndtr/goleveldb/leveldb"

type Snapshot struct {
	reader
	snap *leveldb.Snapshot
}

func (s *Snapshot) Release() {
	s.snap.Release()
}
