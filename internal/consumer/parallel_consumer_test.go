package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"testing"
	"time"
)

func TestParallelConsumer_Do(t *testing.T) {
	testedUnit := &ParallelConsumer{
		threadsNum: 10,
		sinceTime:  time.Time{},
		toTime:     time.Time{},
		offsets:    make(map[int32]int64),
		consumers:  nil,
		interOp:    make(chan *kafka.Message),
		errChan:    make(chan error),
		topic:      "test",
	}

	if testedUnit != nil {
	}
}
