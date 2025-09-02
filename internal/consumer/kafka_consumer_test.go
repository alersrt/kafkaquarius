package consumer

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"testing"
	"time"
)

type testConsumer struct {
	ev kafka.Event
}

func (t *testConsumer) Poll(timeoutMs int) kafka.Event {
	return t.ev
}

func (t *testConsumer) Unassign() error {
	return nil
}

func (t *testConsumer) Close() error {
	return nil
}

func TestKafkaConsumer_Do_Empty(t *testing.T) {
	tests := []struct {
		name string
		ev   kafka.Event
	}{
		{"message", &kafka.Message{}},
		{"timeout", kafka.ErrTimedOut},
		{"error", kafka.ErrFail},
		{"PartitionEOF", kafka.PartitionEOF{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testedUnit := KafkaConsumer{
				toTime:   time.Now(),
				cons:     &testConsumer{test.ev},
				partsNum: 1,
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := testedUnit.Do(ctx, false)
			if err != nil {
				t.Errorf("%v", err)
			}
		})
	}
}

func TestKafkaConsumer_Do_Handler(t *testing.T) {
	tests := []struct {
		name     string
		ev       kafka.Event
		isHandle bool
	}{
		{"message", &kafka.Message{}, true},
		{"timeout", kafka.ErrTimedOut, false},
		{"error", kafka.ErrFail, true},
		{"PartitionEOF", kafka.PartitionEOF{}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			interOp := make(chan kafka.Event)

			testedUnit := KafkaConsumer{
				toTime:   time.Now().Add(time.Hour),
				cons:     &testConsumer{test.ev},
				partsNum: 1,
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			go func() {
				for {
					select {
					case <-ctx.Done():
						if test.isHandle {
							t.Errorf("not handled")
						}
						return
					case <-interOp:
						return
					}
				}
			}()

			go func() {
				err := testedUnit.Do(ctx, false, func(ev kafka.Event) { interOp <- ev })
				if err != nil {
					t.Errorf("%v", err)
				}
			}()

			<-ctx.Done()
		})
	}
}
