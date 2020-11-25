package multierr

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/lovoo/goka/internal/test"
)

func TestErrGroup_Go(t *testing.T) {
	bctx := context.Background()

	// no errors
	g, ctx := NewErrGroup(bctx)
	g.Go(func() error { return nil })
	errs := g.Wait()
	err := errs.NilOrError()
	test.AssertNil(t, err)
	test.AssertNotNil(t, ctx.Err())
	test.AssertStringContains(t, ctx.Err().Error(), "context canceled")

	// with one error
	g, ctx = NewErrGroup(bctx)
	g.Go(func() error { return fmt.Errorf("some error") })
	errs = g.Wait()
	err = errs.NilOrError()
	test.AssertNotNil(t, err)
	test.AssertStringContains(t, err.Error(), "some error")
	test.AssertNotNil(t, ctx.Err())
	test.AssertStringContains(t, ctx.Err().Error(), "context canceled")

	// with one error
	g, ctx = NewErrGroup(bctx)
	g.Go(func() error { return fmt.Errorf("some error") })
	g.Go(func() error { return fmt.Errorf("some error2") })
	errs = g.Wait()
	err = errs.NilOrError()
	test.AssertNotNil(t, err)
	test.AssertStringContains(t, err.Error(), "some error")
	test.AssertStringContains(t, err.Error(), "some error2")
	test.AssertNotNil(t, ctx.Err())
	test.AssertStringContains(t, ctx.Err().Error(), "context canceled")
}

func TestErrGroup_Empty(t *testing.T) {
	g, _ := NewErrGroup(context.Background())

	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		g.Wait()
	}()

	select {
	case <-time.NewTimer(10 * time.Millisecond).C:
		t.Fatalf("waiting for errgroup timed out")
	case <-waitDone:
		log.Printf("wait done")
		// ok
	}
}
