package goka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lovoo/goka/kafka"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/multierr"
	"github.com/lovoo/goka/storage"

	"github.com/Shopify/sarama"
)

const (
	defaultPartitionChannelSize = 10
	stallPeriod                 = 30 * time.Second
	stalledTimeout              = 2 * time.Minute
)

// partition represents one partition of a group table and handles the updates to
// this table via UpdateCallback and ProcessCallback.
//
// partition can be started in two modes:
// - catchup-mode: used by views, starts with startCatchup(), only UpdateCallback called
// - processing-mode: used by processors, starts with start(),
//                    recovers table with UpdateCallback
//                    processes input streams with ProcessCallback
//
// The partition should never be called with a closed storage proxy.
// - Before starting the partition in either way, the client must open the storage proxy.
// - A partition may be restarted even if it returned errors. Before restarting
//   it, the client must call reinit().
// - To release all resources, after stopping the partition, the client must
//   close the storage proxy.
//
type partition struct {
	log       logger.Logger
	topic     string
	partition int32

	ch      chan kafka.Event
	st      *storageProxy
	process processCallback

	hwm             int64
	offset          int64
	recoveredSignal *Signal

	stats         *PartitionStats
	lastStats     *PartitionStats
	requestStats  chan bool
	responseStats chan *PartitionStats
}

type kafkaProxy interface {
	Add(string, int64) error
	Remove(string) error
	AddGroup()
	Stop()
}

const (
	notRecovered State = 0
	recovered    State = 1
)

type processCallback func(msg *message, st storage.Storage, wg *sync.WaitGroup, pstats *PartitionStats) (int, error)

func newPartition(log logger.Logger, topic string, partitionID int32, cb processCallback, st *storageProxy, channelSize int) *partition {
	return &partition{
		log:       log,
		topic:     topic,
		partition: partitionID,

		ch: make(chan kafka.Event, channelSize),
		st: st,

		process: cb,

		recoveredSignal: NewSignal(notRecovered, recovered),

		stats:         newPartitionStats(),
		lastStats:     newPartitionStats(),
		requestStats:  make(chan bool),
		responseStats: make(chan *PartitionStats, 1),
	}
}

// start loads the table partition up to HWM and then consumes streams
func (p *partition) catchupToHwmAndRun(ctx context.Context, consumer sarama.Consumer) error {
	p.stats.Table.StartTime = time.Now()

	// have no state, nothing to catch up from
	if p.st.Stateless() {
		return p.markRecovered(true)
	}

	// catchup until hwm
	return p.load(ctx, consumer, true)
}

// continue
func (p *partition) catchupForever(ctx context.Context, consumer sarama.Consumer) error {
	p.stats.Table.StartTime = time.Now()
	return p.load(ctx, consumer, false)
}

///////////////////////////////////////////////////////////////////////////////
// processing
///////////////////////////////////////////////////////////////////////////////
func newMessage(ev *sarama.ConsumerMessage) *message {
	return &message{
		Topic:     ev.Topic,
		Partition: ev.Partition,
		Offset:    ev.Offset,
		Timestamp: ev.Timestamp,
		Data:      ev.Value,
		Key:       string(ev.Key),
	}
}

func (p *partition) consumeClaim(claim sarama.GroupConsumerClaim) error {
	var wg sync.WaitGroup

	// TODO: do a timeout
	defer wg.Wait()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return
			}
			part.consumeMessage(g.ctx, &wg, msg)
		case <-p.requestStats:
			p.lastStats = newPartitionStats().init(p.stats, p.offset, p.hwm)
			select {
			case p.responseStats <- p.lastStats:
			case <-ctx.Done():
				// Async close is safe to be called multiple times
				partConsumer.AsyncClose()
			}
		case <-ctx.Done():
			// Async close is safe to be called multiple times
			partConsumer.AsyncClose()
		}
	}
}

func (p *partition) consumeMessage(ctx context.Context, wg *sync.WaitGroup, message *sarama.ConsumerMessage) error {

	updates, err := p.process(newMessage(message), p.st, wg, p.stats)
	if err != nil {
		return fmt.Errorf("error processing message: %v", err)
	}
	p.offset += int64(updates)
	p.hwm = p.offset + 1

	// metrics
	s := p.stats.Input[message.Topic]
	s.Count++
	s.Bytes += len(message.Value)
	if !message.Timestamp.IsZero() {
		s.Delay = time.Since(message.Timestamp)
	}
	p.stats.Input[message.Topic] = s
	return nil
}

func (p *partition) dumpStats() *PartitionStats {
	return newPartitionStats().init(p.stats, p.offset, p.hwm)
}

///////////////////////////////////////////////////////////////////////////////
// loading storage
///////////////////////////////////////////////////////////////////////////////

func (p *partition) recovered() bool {
	return p.recoveredSignal.IsState(recovered)
}

func (p *partition) load(ctx context.Context, consumer sarama.Consumer, catchup bool) error {
	// fetch local offset
	var (
		localOffset  int64
		partitionHwm int64
		partConsumer sarama.PartitionConsumer
		err          error
	)
	localOffset, err = p.st.GetOffset(sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("error reading local offset: %v", err)
	}

	hwms := consumer.HighWaterMarks()
	partitionHwm = hwms[p.topic][p.partition]

	if localOffset >= partitionHwm {
		return fmt.Errorf("local offset is higher than partition offset. topic %s, partition %d, hwm %d, local offset %d", p.topic, p.partition, partitionHwm, localOffset)
	}

	// we are exactly where we're supposed to be
	// AND we're here for catchup, so let's stop here
	if localOffset == partitionHwm-1 && catchup {
		return nil
	}

	partConsumer, err = consumer.ConsumePartition(p.topic, p.partition, localOffset)
	if err != nil {
		return fmt.Errorf("Error creating partition consumer for topic %s, partition %d, offset %d: %v", p.topic, p.partition, localOffset, err)
	}

	// reset stats after load
	defer p.stats.reset()

	errs, ctx := multierr.NewErrGroup(ctx)

	errs.Go(func() error {
		errs := new(multierr.Errors)
		for {
			select {
			case consError, ok := <-partConsumer.Errors():
				if !ok {
					break
				}
				errs.Collect(consError)
			}
		}
		return errs.NilOrError()
	})

	errs.Go(func() error {
		stallTicker := time.NewTicker(stallPeriod)
		defer stallTicker.Stop()

		errs := new(multierr.Errors)
		lastMessage := time.Now()

		for {
			select {
			case msg, ok := <-partConsumer.Messages():
				if !ok {
					break
				}

				if p.recoveredSignal.IsState(1) && catchup {
					p.log.Printf("received message in topic %s, partition %s after catchup. Another processor is still producing messages. Ignoring message.", p.topic, p.partition)
					continue
				}

				lastMessage = time.Now()
				if msg.Topic != p.topic {
					return errs.Collect(fmt.Errorf("Got unexpected topic %s, require %s", msg.Topic, p.topic))
				}
				if err := p.storeEvent(string(msg.Key), msg.Value, msg.Offset); err != nil {
					return fmt.Errorf("load: error updating storage: %v", err)
				}
				p.offset = msg.Offset

				if msg.Offset >= partitionHwm-1 {

					if err := p.markRecovered(catchup); err != nil {
						return fmt.Errorf("error setting recovered: %v", err)
					}
					if catchup {
						// close the consumer. We will continue draining errors and messages, until both are closed.
						partConsumer.AsyncClose()
					}
				}

				// update metrics
				s := p.stats.Input[msg.Topic]
				s.Count++
				s.Bytes += len(msg.Value)
				if !msg.Timestamp.IsZero() {
					s.Delay = time.Since(msg.Timestamp)
				}
				p.stats.Input[msg.Topic] = s
				p.stats.Table.Stalled = false
			case now := <-stallTicker.C:
				// only set to stalled, if the last message was earlier
				// than the stalled timeout
				if now.Sub(lastMessage) > stalledTimeout {
					p.stats.Table.Stalled = true
				}

			case <-p.requestStats:
				p.lastStats = newPartitionStats().init(p.stats, p.offset, p.hwm)
				select {
				case p.responseStats <- p.lastStats:
				case <-ctx.Done():
					// Async close is safe to be called multiple times
					partConsumer.AsyncClose()
				}
			case <-ctx.Done():
				// Async close is safe to be called multiple times
				partConsumer.AsyncClose()
			}
		}

		return errs.NilOrError()
	})

	return errs.Wait().NilOrError()
}

func (p *partition) storeEvent(key string, value []byte, offset int64) error {
	err := p.st.Update(key, value)
	if err != nil {
		return fmt.Errorf("Error from the update callback while recovering from the log: %v", err)
	}
	err = p.st.SetOffset(offset)
	if err != nil {
		return fmt.Errorf("Error updating offset in local storage while recovering from the log: %v", err)
	}
	return nil
}

// mark storage as recovered
func (p *partition) markRecovered(catchup bool) error {
	// already recovered
	if p.recoveredSignal.IsState(1) {
		return nil
	}

	p.lastStats = newPartitionStats().init(p.stats, p.offset, p.hwm)
	p.lastStats.Table.Status = PartitionPreparing

	// mark storage as recovered -- this may take long
	// TODO: have a ticker that says we're still waiting for the storage to
	// be marked as recoverered
	if err := p.st.MarkRecovered(); err != nil {
		return fmt.Errorf("Error marking storage recovered topic %s, partition %s: %v", p.topic, p.partition, err)
	}

	// update stats
	p.stats.Table.Status = PartitionRunning
	p.stats.Table.RecoveryTime = time.Now()

	p.recoveredSignal.SetState(1)

	// Be sure to mark partition as not stalled after EOF arrives, as
	// this will not be written in the run-method
	p.stats.Table.Stalled = false

	return nil
}

func (p *partition) fetchStats(ctx context.Context) *PartitionStats {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case p.requestStats <- true:
	case <-ctx.Done():
		return newPartitionStats().init(p.lastStats, p.offset, p.hwm)
	case <-timer.C:
		return p.lastStats
	}

	select {
	case s := <-p.responseStats:
		return s
	case <-ctx.Done():
		return newPartitionStats().init(p.lastStats, p.offset, p.hwm)
	}
}
