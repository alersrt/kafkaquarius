package consumer

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"sync"
	"time"
)

type Consumer interface {
	Do(ctx context.Context, target chan *kafka.Message, errChan chan error) error
	Close()
}

type ParallelConsumer struct {
	threadsNum int
	sinceTime  time.Time
	toTime     time.Time
	offsets    map[int32]int64
	consumers  []Consumer
	interOp    chan *kafka.Message
	errChan    chan error
	topic      string
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
		interOp:    make(chan *kafka.Message),
		errChan:    make(chan error),
		consumers:  workers,
		topic:      topic,
	}, nil
}

func (p *ParallelConsumer) Close() {
	for _, cons := range p.consumers {
		cons.Close()
	}
}

func (p *ParallelConsumer) Do(ctx context.Context, errProc func(err error), procs ...func(*kafka.Message)) error {
	ctx, cancel := context.WithCancelCause(ctx)
	var wg sync.WaitGroup
	for _, c := range p.consumers {
		wg.Add(1)
		go func() {
			err := c.Do(ctx, p.interOp, p.errChan)
			if err != nil {
				cancel(err)
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		cancel(nil)
	}()

	for {
		select {
		case <-ctx.Done():
			if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, ctx.Err()) {
				return cause
			} else {
				return nil
			}
		case msg := <-p.interOp:
			p.offsets[msg.TopicPartition.Partition] = int64(msg.TopicPartition.Offset)
			for _, proc := range procs {
				proc(msg)
			}
		case err := <-p.errChan:
			errProc(err)
		}
	}
}
