package goka

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/lovoo/goka/storage/null"
)

func newMockOptions(t *testing.T) *poptions {
	opts := new(poptions)
	err := opts.applyOptions("")
	ensure.Err(t, err, regexp.MustCompile("StorageBuilder not set$"))

	opts.builders.storage = null.Builder
	err = opts.applyOptions("")
	ensure.Nil(t, err)

	fmt.Printf("%+v\n", opts)
	return opts
}
