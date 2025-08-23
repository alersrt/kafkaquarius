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
&& Timestamp < timestamp('2026-01-01')
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
		t.Errorf("test: filter: eval: %+v", err)
	}
	if !ok {
		t.Errorf("test: filter: expected true")
	}
}
