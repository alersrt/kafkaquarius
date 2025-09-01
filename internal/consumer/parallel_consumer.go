package consumer

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

type Consumer interface {
	Do(ctx context.Context, isEndless bool, errProc func(kafka.Error), handles ...func(*kafka.Message)) error
	Close()
}

type ParallelConsumer struct {
	threadsNum int
	isEndless  bool
	sinceTime  time.Time
	toTime     time.Time
	offsets    map[int32]int64
	offsetMtx  sync.Mutex
	consumers  []Consumer
	topic      string
	activeCons atomic.Int32
}

func NewParallelConsumer(threadsNum int, sinceTime time.Time, toTime time.Time, topic string, kafkaCfg kafka.ConfigMap) (*ParallelConsumer, error) {
	workers := make([]Consumer, threadsNum)
	for i := 0; i < threadsNum; i++ {
		var err error
		workers[i], err = NewKafkaConsumer(topic, i, threadsNum, sinceTime, toTime, kafkaCfg)
		if err != nil {
			return nil, err
		}
	}
	return &ParallelConsumer{
		threadsNum: threadsNum,
		sinceTime:  sinceTime,
		toTime:     toTime,
		offsets:    make(map[int32]int64),
		consumers:  workers,
		topic:      topic,
	}, nil
}

func (p *ParallelConsumer) Threads() int32 {
	return p.activeCons.Load()
}

func (p *ParallelConsumer) Offsets() map[int32]int64 {
	return maps.Clone(p.offsets)
}

func (p *ParallelConsumer) StoreOffset(partition int32, offset int64) {
	p.offsetMtx.Lock()
	defer p.offsetMtx.Unlock()
	p.offsets[partition] = offset
}

func (p *ParallelConsumer) Close() {
	for _, cons := range p.consumers {
		cons.Close()
	}
}

func (p *ParallelConsumer) Do(ctx context.Context, errProc func(err error), procs ...func(*kafka.Message)) error {
	ctx, cancel := context.WithCancelCause(ctx)

	interOp := make(chan *kafka.Message)
	defer close(interOp)
	errChan := make(chan error)
	defer close(errChan)

	var wg sync.WaitGroup
	for _, c := range p.consumers {
		wg.Add(1)
		p.activeCons.Add(1)
		go func() {
			err := c.Do(ctx, p.isEndless, func(err kafka.Error) { errChan <- err }, func(msg *kafka.Message) { interOp <- msg })
			if err != nil {
				cancel(err)
			}
			wg.Done()
			p.activeCons.Add(-1)
		}()
	}

	go func() {
		wg.Wait()
		cancel(nil)
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-interOp:
				p.StoreOffset(msg.TopicPartition.Partition, int64(msg.TopicPartition.Offset))
				for _, proc := range procs {
					proc(msg)
				}
			case err := <-errChan:
				errProc(err)
			}
		}
	}()

	<-ctx.Done()
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, ctx.Err()) {
		return cause
	} else {
		return nil
	}
}
