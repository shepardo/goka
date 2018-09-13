package simple

import (
	"github.com/lovoo/goka/storage"
)

func Builder(topic string, partition int32) (storage.Storage, error) {
	return New(), nil
}
