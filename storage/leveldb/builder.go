package leveldb

import (
	"fmt"
	"path/filepath"

	"github.com/lovoo/goka/storage"
	"github.com/syndtr/goleveldb/leveldb"
)

func Builder(path string) storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		fp := filepath.Join(path, fmt.Sprintf("%s.%d", topic, partition))
		db, err := leveldb.OpenFile(fp, nil)
		if err != nil {
			return nil, fmt.Errorf("error opening leveldb: %v", err)
		}

		return New(db)
	}
}

// DefaultBuilder builds a LevelDB storage with default configuration.
// The database will be stored in the given path.
/*func DefaultBuilder(path string) Builder {
	return func(topic string, partition int32) (*Storage, error) {
		fp := filepath.Join(path, fmt.Sprintf("%s.%d", topic, partition))
		db, err := leveldb.OpenFile(fp, nil)
		if err != nil {
			return nil, fmt.Errorf("error opening leveldb: %v", err)
		}

		return New(db)
	}
}

// BuilderWithOptions builds LevelDB storage with the given options and
// in the given path.
func BuilderWithOptiono(path string, opts *opt.Options) Builder {
	return func(topic string, partition int32) (*Storage, error) {
		fp := filepath.Join(path, fmt.Sprintf("%s.%d", topic, partition))
		db, err := leveldb.OpenFile(fp, opts)
		if err != nil {
			return nil, fmt.Errorf("error opening leveldb: %v", err)
		}

		return New(db)
	}
}*/
