package consumer

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ParallelConsumer struct {
	threadsNum  int
	isEndless   bool
	sinceTime   time.Time
	toTime      time.Time
	offsets     map[int32]int64
	offsetRWMtx sync.RWMutex
	consumers   []*KafkaConsumer
	topic       string
	stats       struct {
		activeCons atomic.Int32
	}
}

func NewParallelConsumer(threadsNum int, sinceTime time.Time, toTime time.Time, topic string, configMap kafka.ConfigMap) (*ParallelConsumer, error) {
	err := configMap.SetKey("client.id", "init")
	if err != nil {
		return nil, err
	}
	cons, err := kafka.NewConsumer(&configMap)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = cons.Close()
	}()
	timeoutMs := 5 * time.Minute.Milliseconds()

	md, err := cons.GetMetadata(&topic, false, int(timeoutMs))
	if err != nil {
		return nil, err
	}

	var parts []kafka.TopicPartition
	for i := range md.Topics[topic].Partitions {
		parts = append(parts, kafka.TopicPartition{
			Topic:     &topic,
			Partition: int32(i),
			Offset:    kafka.Offset(sinceTime.UnixMilli()),
		})
	}

	parts, err = cons.OffsetsForTimes(parts, int(timeoutMs))
	if err != nil {
		return nil, err
	}

	workers := make([]*KafkaConsumer, threadsNum)
	for i := 0; i < threadsNum; i++ {
		var err error
		evalParts := calcParts(i, len(parts), threadsNum)
		consParts := make([]kafka.TopicPartition, len(evalParts))
		for i := 0; i < len(consParts); i++ {
			consParts[i] = parts[evalParts[i]]
		}
		workers[i], err = NewKafkaConsumer(i, toTime, consParts, configMap)
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
	return p.stats.activeCons.Load()
}

func (p *ParallelConsumer) Offsets() map[int32]int64 {
	p.offsetRWMtx.RLock()
	defer p.offsetRWMtx.RUnlock()
	return maps.Clone(p.offsets)
}

func (p *ParallelConsumer) StoreOffset(partition int32, offset int64) {
	p.offsetRWMtx.Lock()
	defer p.offsetRWMtx.Unlock()
	p.offsets[partition] = offset
}

func (p *ParallelConsumer) Close() {
	for _, cons := range p.consumers {
		cons.Close()
	}
}

func (p *ParallelConsumer) Do(ctx context.Context, errProc func(err error), procs ...func(*kafka.Message)) error {
	var (
		err    error
		errMtx sync.Mutex
		wg     sync.WaitGroup
	)

	for _, c := range p.consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.stats.activeCons.Add(1)
			defer p.stats.activeCons.Add(-1)

			cErr := c.Do(ctx, p.isEndless, func(ev kafka.Event) {
				switch ev := ev.(type) {
				case kafka.Error:
					errProc(ev)
				case *kafka.Message:
					p.StoreOffset(ev.TopicPartition.Partition, int64(ev.TopicPartition.Offset))
					for _, proc := range procs {
						proc(ev)
					}
				}
			})
			if cErr != nil {
				errMtx.Lock()
				defer errMtx.Unlock()
				err = errors.Join(err, cErr)
			}
		}()
	}

	wg.Wait()
	return err
}

// calcParts returns list with partition numbers. Numeration is started from zero.
func calcParts(threadNo int, partsNum int, threadsNum int) []int {
	if threadNo >= partsNum || threadNo >= threadsNum || threadNo < 0 {
		return nil
	}
	if threadsNum >= partsNum {
		return []int{threadNo}
	}
	// div is always >= 1 due to previous condition
	div := partsNum / threadsNum
	var resLen int
	if threadsNum*div+threadNo < partsNum {
		resLen = div + 1
	} else {
		resLen = div
	}
	res := make([]int, resLen)
	for i := 0; i < resLen; i++ {
		res[i] = threadNo + threadsNum*i
	}
	return res
}
