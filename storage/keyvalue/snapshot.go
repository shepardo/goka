package keyvalue

import "github.com/lovoo/goka/storage/keyvalue/backend"

type Snapshot struct {
	reader
	snapshot backend.Snapshot
}

func (s *Snapshot) Release() {
	s.snapshot.Release()
}
