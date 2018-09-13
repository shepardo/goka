package leveldb

import "github.com/syndtr/goleveldb/leveldb"

// snapshot represents a LevelDB snapshot. It must be cleaned up after use by
// calling Release().
type snapshot struct {
	readSupport
	snap *leveldb.Snapshot
}

func newSnapshot(snap *leveldb.Snapshot) *snapshot {
	return &snapshot{readSupport{snap}, snap}
}

func (s *snapshot) Release() {
	s.snap.Release()
}
