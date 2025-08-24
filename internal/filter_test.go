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
&& Key.kf == 'test_key'
&& Ts > timestamp('1970-01-01T00:00:00.000Z')
`
	testedUnit, err := NewFilter(filterContent)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	data, err := FromKafka(&kafka.Message{
		Key:   []byte("{\"kf\":\"test_key\"}"),
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
