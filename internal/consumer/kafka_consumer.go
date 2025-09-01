package consumer

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"sync"
	"time"
)

type KafkaConsumer struct {
	topic        string
	threadNo     int
	sinceTime    time.Time
	toTime       time.Time
	cons         *kafka.Consumer
	parts        []kafka.TopicPartition
	reachedMutex sync.Mutex
}

func NewKafkaConsumer(topic string, threadNo int, threadsNum int, sinceTime time.Time, toTime time.Time, configMap kafka.ConfigMap) (*KafkaConsumer, error) {
	err := configMap.SetKey("client.id", threadNo)
	if err != nil {
		return nil, err
	}
	cons, err := kafka.NewConsumer(&configMap)
	if err != nil {
		return nil, err
	}

	timeoutMs := 5 * time.Minute.Milliseconds()

	md, err := cons.GetMetadata(&topic, false, int(timeoutMs))
	if err != nil {
		return nil, err
	}

	partsDist := calcParts(threadNo, len(md.Topics[topic].Partitions), threadsNum)
	if partsDist == nil {
		return nil, fmt.Errorf("kafka consumer: no partitions")
	}

	var parts []kafka.TopicPartition
	for _, p := range partsDist {
		parts = append(parts, kafka.TopicPartition{
			Topic:     &topic,
			Partition: int32(p),
			Offset:    kafka.Offset(sinceTime.UnixMilli()),
		})
	}

	parts, err = cons.OffsetsForTimes(parts, int(timeoutMs))
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		topic:     topic,
		threadNo:  threadNo,
		sinceTime: sinceTime,
		toTime:    toTime,
		cons:      cons,
		parts:     parts,
	}, nil
}

func (c *KafkaConsumer) Close() {
	_ = c.cons.Unassign()
	_ = c.cons.Close()
}

func (c *KafkaConsumer) Do(ctx context.Context, isEndless bool, handles ...func(kafka.Event)) error {
	err := c.cons.Assign(c.parts)
	if err != nil {
		return err
	}

	partsNum := len(c.parts)
	finalCnt := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			ev := c.cons.Poll(int(time.Minute.Milliseconds()))
			switch ev.(type) {
			case *kafka.Message:
				if c.toTime.Before(ev.(*kafka.Message).Timestamp) {
					return nil
				}
			case kafka.Error:
				if !isEndless && ev.(kafka.Error).IsTimeout() {
					return nil
				}
			case kafka.PartitionEOF:
				finalCnt++
				if !isEndless && finalCnt == partsNum {
					return nil
				}
			}
			for _, handle := range handles {
				handle(ev)
			}
		}
	}
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
