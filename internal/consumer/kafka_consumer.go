package consumer

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"time"
)

type KafkaConsumer struct {
	topic     string
	threadNo  int
	sinceTime time.Time
	toTime    time.Time
	cons      *kafka.Consumer
	parts     []kafka.TopicPartition
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

func (c *KafkaConsumer) Do(ctx context.Context, target chan *kafka.Message, errChan chan error) error {
	err := c.cons.Assign(c.parts)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := c.cons.ReadMessage(time.Minute)
			if err != nil {
				if err != nil && err.(kafka.Error).IsTimeout() {
					return nil
				}
				if errChan != nil {
					errChan <- err
				}
				continue
			}
			if c.toTime.Before(msg.Timestamp) {
				return nil
			}

			target <- msg
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
