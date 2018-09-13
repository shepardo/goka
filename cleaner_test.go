package goka

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
)

func TestNoCleaningUp(t *testing.T) {
	cleaner := NoCleaning(context.Background())
	select {
	case <-cleaner:
		t.Fatalf("didn't expect cleaner to trigger")
	default:
	}
}

func TestPeriodicCleanUp(t *testing.T) {
	clk := clock.NewMock()
	ctx, cancel := context.WithCancel(context.Background())
	cleaner := periodicCleaning(time.Second, clk)(ctx)

	t.Run("Not Triggered on Creation", func(t *testing.T) {
		select {
		case <-cleaner:
			t.Fatalf("clean up shouldn't trigger on creation")
		default:
		}
	})

	clk.Add(time.Second)

	var cb CleanerCallback
	t.Run("Triggers After Specified Duration", func(t *testing.T) {
		select {
		case cb = <-cleaner:
		default:
			t.Fatalf("expected cleaner to trigger after the specified duration")
		}
	})

	t.Run("Adjusts for Slow Cleaning", func(t *testing.T) {
		clk.Add(time.Second)

		select {
		case <-cleaner:
			t.Fatalf("expected cleaner to not trigger before calling callback")
		default:
		}

		clk.Add(time.Second)
		cb()

		select {
		case <-cleaner:
			t.Fatalf("expected cleaner not to trigger immediately after callback")
		default:
		}

		clk.Add(time.Second)

		select {
		case <-cleaner:
		default:
			t.Fatalf("expected cleaner to trigger after duration and callback")
		}
	})

	t.Run("Context Cancellation", func(t *testing.T) {
		timeout := time.AfterFunc(time.Second, func() { t.Fatalf("expected cleaner to close") })
		defer timeout.Stop()

		cancel()

		select {
		case _, ok := <-cleaner:
			if ok {
				t.Fatalf("context cancellation should close cleaner")
			}
		case <-timeout.C:
		}
	})
}
