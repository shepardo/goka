package systemtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/storage"
)

func pollTimed(t *testing.T, what string, secTimeout float64, pollers ...func() bool) {
	for i := 0; i < int(secTimeout/0.02); i++ {
		var ok = true
		for _, poller := range pollers {
			if !poller() {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waiting for %s timed out", what)
}

func hashKey(key string, numPartitions int) int32 {
	hasher := goka.DefaultHasher()()
	hasher.Write([]byte(key))
	hash := int32(hasher.Sum32())
	if hash < 0 {
		hash = -hash
	}
	return int32(int(hash) % numPartitions)
}

type storageTracker struct {
	storages map[string]storage.Storage
}

func newStorageTracker() *storageTracker {
	return &storageTracker{
		storages: make(map[string]storage.Storage),
	}
}

func (st *storageTracker) Build(topic string, partition int32) (storage.Storage, error) {
	key := st.key(topic, partition)
	if existing, ok := st.storages[key]; ok {
		return existing, nil
	}
	st.storages[key] = storage.NewMemory()
	return st.storages[key], nil
}

func (st *storageTracker) key(topic string, partition int32) string {
	return fmt.Sprintf("%s.%d", topic, partition)
}
