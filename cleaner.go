package goka

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"
)

// CleanerCallback is a function that will be called to signal cleanning
// finishing.
type CleanerCallback func()

// CleanerPolicy decides when clean up should be performed on the local cache.
// It signals a clean up by sending a CleanerCallback down the channel.
// CleanerPolicy is called once per topic/partition and should free its
// resources once the context is canceled.
type CleanerPolicy func(context.Context) <-chan CleanerCallback

// NoCleaning is a CleanerPolicy that will never initiate a clean up.
func NoCleaning(ctx context.Context) <-chan CleanerCallback { return nil }

// PeriodicCleaning returns a CleanerPolicy that will periodically clean up the
// the local cache. It adjusts for slow clean ups by only signaling the next
// clean up after the specified duration since last clean up has passed. First
// clean up will be run after the specified duration has passed.
func PeriodicCleaning(d time.Duration) CleanerPolicy {
	return periodicCleaning(d, clock.New())
}

func periodicCleaning(d time.Duration, clk clock.Clock) CleanerPolicy {
	return func(ctx context.Context) <-chan CleanerCallback {
		done := ctx.Done()
		ch := make(chan CleanerCallback)
		timer := clk.Timer(d)

		go func() {
			for {
				select {
				case <-timer.C:
					ch <- func() { timer.Reset(d) }
				case <-done:
					timer.Stop()
					close(ch)
					return
				}
			}
		}()

		return ch
	}
}
