package consumer

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParallelConsumer_Do(t *testing.T) {
	tests := []struct {
		name     string
		ev       kafka.Event
		isHandle bool
	}{
		{"message", &kafka.Message{Timestamp: time.Now().Add(time.Hour), TopicPartition: kafka.TopicPartition{Partition: 1}}, false},
		{"message", &kafka.Message{Timestamp: time.Now().Add(-time.Hour), TopicPartition: kafka.TopicPartition{Partition: 1}}, true},
		{"timeout", kafka.NewError(kafka.ErrTimedOut, "", false), false},
		{"error", kafka.NewError(kafka.ErrFail, "", false), true},
		{"PartitionEOF", kafka.PartitionEOF{}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			threadsNum := 3
			partsNum := 10
			consumers := make([]*KafkaConsumer, threadsNum)
			for i := 0; i < threadsNum; i++ {
				evalParts := calcParts(i, partsNum, threadsNum)
				consumers[i] = &KafkaConsumer{
					toTime: time.Now(),
					cons:   &testConsumer{test.ev},
					parts:  make([]kafka.TopicPartition, len(evalParts)),
				}
			}
			testedUnit := &ParallelConsumer{
				threadsNum:  0,
				isEndless:   false,
				sinceTime:   time.Unix(0, 0),
				toTime:      time.Now(),
				offsets:     map[int32]int64{0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0},
				offsetRWMtx: sync.RWMutex{},
				consumers:   consumers,
				topic:       "test",
				stats:       struct{ activeCons atomic.Int32 }{},
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				err := testedUnit.Do(ctx,
					func(err error) {
						if !test.isHandle {
							t.Errorf("shouldn't handle")
						}
					},
					func(msg *kafka.Message) {
						if !test.isHandle {
							t.Errorf("shouldn't handle")
						}
					},
				)
				if err != nil {
					t.Errorf("%v", err)
				}
			}()

			<-ctx.Done()
		})
	}
}

func Test_calcPart(t *testing.T) {
	tests := []struct {
		name       string
		threadNo   int
		partsNum   int
		threadsNum int
		exp        []int
	}{
		// 3/5
		{"", 0, 3, 5, []int{0}},
		{"", 2, 3, 5, []int{2}},
		{"", 3, 3, 5, nil},
		// 5/5
		{"5/5", 0, 5, 5, []int{0}},
		{"5/5", 4, 5, 5, []int{4}},
		// 5/3
		{"5/3", 0, 5, 3, []int{0, 3}},
		{"5/3", 1, 5, 3, []int{1, 4}},
		{"5/3", 2, 5, 3, []int{2}},
		// 10/5
		{"10/5", 0, 10, 5, []int{0, 5}},
		{"10/5", 4, 10, 5, []int{4, 9}},
		// 8/3
		{"8/3", 0, 8, 3, []int{0, 3, 6}},
		{"8/3", 1, 8, 3, []int{1, 4, 7}},
		{"8/3", 2, 8, 3, []int{2, 5}},
		// 5/4
		{"5/4", 0, 5, 4, []int{0, 4}},
		{"5/4", 1, 5, 4, []int{1}},
		{"5/4", 2, 5, 4, []int{2}},
		{"5/4", 3, 5, 4, []int{3}},
		// 27/12
		{"27/12", 0, 27, 12, []int{0, 12, 24}},
		{"27/12", 2, 27, 12, []int{2, 14, 26}},
		{"27/12", 3, 27, 12, []int{3, 15}},
		{"27/12", 11, 27, 12, []int{11, 23}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			act := calcParts(test.threadNo, test.partsNum, test.threadsNum)
			if !reflect.DeepEqual(test.exp, act) {
				t.Errorf("exp: %+v, act: %v", test.exp, act)
			}
		})
	}
}
