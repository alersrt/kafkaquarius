package consumer

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"time"
)

type Consumer interface {
	Poll(timeoutMs int) kafka.Event
	Unassign() error
	Close() error
}

type KafkaConsumer struct {
	toTime   time.Time
	cons     Consumer
	partsNum int
}

func NewKafkaConsumer(threadNo int, toTime time.Time, parts []kafka.TopicPartition, configMap kafka.ConfigMap) (*KafkaConsumer, error) {
	err := configMap.SetKey("client.id", threadNo)
	if err != nil {
		return nil, err
	}
	cons, err := kafka.NewConsumer(&configMap)
	if err != nil {
		return nil, err
	}

	err = cons.Assign(parts)
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		toTime:   toTime,
		cons:     cons,
		partsNum: len(parts),
	}, nil
}

func (c *KafkaConsumer) Close() {
	_ = c.cons.Unassign()
	_ = c.cons.Close()
}

func (c *KafkaConsumer) Do(ctx context.Context, isEndless bool, handles ...func(kafka.Event)) error {
	finalCnt := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			ev := c.cons.Poll(int(time.Minute.Milliseconds()))
			switch ev := ev.(type) {
			case *kafka.Message:
				if c.toTime.Before(ev.Timestamp) {
					return nil
				}
			case kafka.Error:
				if !isEndless && ev.IsTimeout() {
					return nil
				}
			case kafka.PartitionEOF:
				finalCnt++
				if !isEndless && finalCnt == c.partsNum {
					return nil
				}
			}
			for _, handle := range handles {
				handle(ev)
			}
		}
	}
}
