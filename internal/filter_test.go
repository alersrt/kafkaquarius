package internal

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"testing"
	"time"
)

func TestFilter(t *testing.T) {
	filterContent := `
Value.some == 0
&& Headers.key1 == 'value'
&& Headers.size() != 0
//&& Ts < timestamp('2026-01-01T00:00:00.000')
`
	testedUnit, err := NewFilter(filterContent)
	if err != nil {
		t.Fatalf("test: filter: new: %+v", err)
	}

	data, err := FromKafka(&kafka.Message{
		Key:   []byte("test_key"),
		Value: []byte("{\"some\":0}"),
		Headers: []kafka.Header{{
			Key:   "key1",
			Value: []byte("value"),
		}},
		Timestamp: time.Now(),
	})

	ok, err := testedUnit.Eval(data)
	if err != nil {
		t.Errorf("%+v", err)
	}
	if !ok {
		t.Errorf("expected true")
	}
}
